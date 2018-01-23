package com.ibm.aspen.core.read

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.ClientSideReadMessenger
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.objects.StorePointer
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.core.objects.ObjectEncodingError
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.ObjectState
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectStoreState
import com.ibm.aspen.core.data_store.Lock

class BaseReadDriver(
    val clientMessenger: ClientSideReadMessenger,
    val objectPointer: ObjectPointer,
    val retrieveObjectData: Boolean,
    val retrieveLockedTransaction: Boolean, 
    val readUUID:UUID)(implicit ec: ExecutionContext) extends ReadDriver {
  
  import BaseReadDriver._
  
  protected val promise = Promise[Either[ReadError, (ObjectState, Option[Map[DataStoreID, List[Lock]]])]]
  
  protected var storeStates = Map[DataStoreID, StoreState]()
  protected var errors = Map[DataStoreID, ReadError.Value]()
  
  def readResult = promise.future
  
  def begin() = sendReadRequests()
  
  /** Sends a Read request to the specified store. Must be called from within a synchronized block */
  protected def sendReadRequest(dataStoreId: DataStoreID): Unit = clientMessenger.send(dataStoreId, 
      Read(dataStoreId, clientMessenger.clientId, readUUID, objectPointer, retrieveObjectData, retrieveLockedTransaction))
      
  def receivedReplyFrom(storeId: DataStoreID): Boolean = synchronized {
    storeStates.contains(storeId) || errors.contains(storeId)
  }
      
  /** Sends a Read request to all stores that have not already responded. May be called outside a synchronized block */
  protected def sendReadRequests(): Unit = {
    objectPointer.storePointers.foreach(sp => {
      val storeId = DataStoreID(objectPointer.poolUUID, sp.poolIndex)
      if (!receivedReplyFrom(storeId))
        sendReadRequest(storeId)
    })
  }
  
  def receiveReadResponse(response:ReadResponse): Unit = synchronized {
    if (promise.isCompleted)
      return // Already done
      
    response.result match {
      case Left(err) => 
        errors += (response.fromStore -> err)
        
        if (storeStates.contains(response.fromStore))
          storeStates -= response.fromStore
          
      case Right(cs) => try {
        // KeyValue objects must take the set of update UUID into account as well as the current ObjectRevision when determining when a consistent
        // read threshold has been achieved.
        
        val updateSet = objectPointer match {
          case _: KeyValueObjectPointer => cs.objectData match {
            case None => cs.updates
            case Some(db) => KeyValueObjectCodec.getUpdateSet(db)
          }
          case _ => Set[UUID]() 
        }
        
        val ss = StoreState(response.fromStore, (cs.revision, updateSet), cs.refcount, cs.timestamp, cs.objectData, cs.locks)
        
        storeStates += (response.fromStore -> ss)
        
        if (errors.contains(response.fromStore))
          errors -= response.fromStore
          
      } catch {
        case t: ObjectEncodingError =>
          errors += (response.fromStore -> ReadError.InvalidObjectEncoding)
          
          if (storeStates.contains(response.fromStore))
            storeStates -= response.fromStore
      }
    }
    
    if ( errors.size >= objectPointer.ida.width - objectPointer.ida.restoreThreshold ) {
      promise.success(Left(new ThresholdError(errors)))
    }
    else if (storeStates.size >= objectPointer.ida.consistentRestoreThreshold)
      checkComplete()
  }
    
  /** Checks the set of received responses to see if a consistent read has been achieved and initiates
   *  re-reads if not.
   *  
   * This method is not invoked until an ida.consistentRestoreThreshold number of store states is received
   * We'll count the responses for each (ObjectRevision, Set[UUID]) tuple and if one of them has more responses
   * than the others, we'll discard the lower-counted responses and re-read those stores. We'll simply repeat
   * this process until a single (ObjectRevision, Set[UUID]) reaches the consistentRestoreThreshold.
   */
  private def checkComplete(): Unit = { 
      
    val revisionCounts = storeStates.values.foldLeft(Map[(ObjectRevision, Set[UUID]),Int]()) { (m, ss) =>
      m + (ss.revision -> (m.getOrElse(ss.revision, 0) + 1))
    }
    
    if (revisionCounts.size > 1) {
      val sortedCounts = revisionCounts.toList.sortBy( t => - t._2 )
      
      if (sortedCounts.head._2 == sortedCounts.tail.head._2)
        return // No definitive winner. Wait for more responses
      
      val pruneSet = sortedCounts.drop(1).map( t => t._1 ).toSet
      
      storeStates foreach { t =>
        val (storeId, ss) = t
        
        if (pruneSet.contains(ss.revision)) {
          storeStates -= storeId
          sendReadRequest(storeId)
        }
      }
    }
    
    // At this point, only a single revision is left in the storeStates map.
    
    if (storeStates.size >= objectPointer.ida.consistentRestoreThreshold) {
      // restore threshold reached. Decode object
      try {
        
        val objectState = objectPointer match {
          case op: DataObjectPointer     => restoreDataObject()
          case op: KeyValueObjectPointer => restoreKeyValueObject()
        }
        
        promise.success(Right(objectState))
        
      } catch {
        case e: IDAError => promise.success(Left(e))
        case e: ObjectEncodingError => promise.success(Left(new EncodingError))
        case err: Throwable =>
          println(s"*** This should never happen. Unexpected Exception: $err")
          com.ibm.aspen.util.printStack()
          promise.success(Left(new UnexpectedError))
      }
    }
  }
  
  def getMetadata = {
    // The current refcount is the one with the highest updateSerial
    val refcount = storeStates.foldLeft(ObjectRefcount(0,0))((ref, t) => if (t._2.refcount.updateSerial > ref.updateSerial) t._2.refcount else ref)
    val revision = storeStates.head._2.revision
    val timestamp = storeStates.head._2.timestamp
    val locks = if (!retrieveLockedTransaction) None else {
      val lockMap = storeStates.foldLeft(Map[DataStoreID, List[Lock]]()) { (m, t) => if (t._2.locks.isEmpty) m else m + (t._1 -> t._2.locks) }
      Some(lockMap)
    }
    (revision._1, refcount, timestamp, locks)
  }
  
  private def restoreDataObject() : (ObjectState, Option[Map[DataStoreID, List[Lock]]]) = {
    val segments = storeStates.foldLeft(List[(Byte, Option[DataBuffer])]()) { (l, t) =>
      (t._1.poolIndex, t._2.objectData) :: l
    }
    // If something goes wrong with the IDA, it'll throw an IDAError exception
    val data = if (retrieveObjectData) objectPointer.ida.restore(segments) else DataBuffer(new Array[Byte](0))
    
    val (revision, refcount, timestamp, locks) = getMetadata
    
    (DataObjectState(objectPointer, revision, refcount, timestamp, data), locks)
  }
  
  private def restoreKeyValueObject(): (ObjectState, Option[Map[DataStoreID, List[Lock]]]) = {
    // Decode to KeyValueObjectStoreState
    val kvosList = storeStates.valuesIterator.filter( ss => ss.objectData.isDefined ).foldLeft(List[KeyValueObjectStoreState]()) { (l, ss) => 
      KeyValueObjectStoreState(ss.storeId.poolIndex, ss.objectData.get) :: l 
    }
    
    val (revision, refcount, timestamp, locks) = getMetadata
    
    (KeyValueObjectCodec.decode(objectPointer, revision, refcount, timestamp, kvosList), locks)
  }
}

object BaseReadDriver {
  case class StoreState(
      storeId: DataStoreID,
      revision: (ObjectRevision, Set[UUID]),
      refcount: ObjectRefcount,
      timestamp: HLCTimestamp,
      objectData: Option[DataBuffer],
      locks: List[Lock])
      
  def noErrorRecoveryReadDriver(ec: ExecutionContext)(
      clientMessenger: ClientSideReadMessenger,
      objectPointer: ObjectPointer,
      retrieveObjectData: Boolean,
      retrieveLockedTransaction: Boolean,
      readUUID:UUID): ReadDriver = {
    new BaseReadDriver(clientMessenger, objectPointer, retrieveObjectData, retrieveLockedTransaction, readUUID)(ec)
  }
}