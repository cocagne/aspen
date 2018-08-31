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
import com.ibm.aspen.core.objects.MetadataObjectState
import com.ibm.aspen.core.ida.IDAError
import com.ibm.aspen.core.data_store
import com.ibm.aspen.core.data_store.ObjectReadError
import scala.concurrent.duration._
import com.ibm.aspen.core.objects.KeyValueObjectState
import org.apache.logging.log4j.scala.Logging
import com.ibm.aspen.core.objects.keyvalue.Key

class BaseReadDriver(
    val getTransactionResult: (UUID) => Option[Boolean],
    val clientMessenger: ClientSideReadMessenger,
    val objectPointer: ObjectPointer,
    val readType: ReadType,
    val retrieveLockedTransaction: Boolean, 
    val readUUID:UUID,
    val opportunisticRebuildDelay: Duration = BaseReadDriver.DefaultOpportunisticRebuildDelay,
    val disableOpportunisticRebuild: Boolean = false
    )(implicit ec: ExecutionContext) extends ReadDriver with Logging {
  
  import BaseReadDriver._
  
  // Detect invalid combinations
  (objectPointer, readType) match {
    case (_:DataObjectPointer,     _:SingleKey)          => assert(false)
    case (_:DataObjectPointer,     _:LargestKeyLessThan) => assert(false)
    case (_:DataObjectPointer,     _:KeyRange)           => assert(false)
    case (_:KeyValueObjectPointer, _:ByteRange)          => assert(false)
    case _ =>
  }
  
  protected val promise = Promise[Either[ReadError, ObjectState]]
  
  protected var storeStates = Map[DataStoreID, StoreState]()
  protected var errors = Map[DataStoreID, ObjectReadError.Value]()
  
  // Tuple is restoredObject plus the set of ObjectRevision for all contents of KeyValue objects. Empty set for DataObjects
  protected var restoredObject: Option[(ObjectState, Set[ObjectRevision])] = None
  protected var retryCount = 0
  
  protected val readStartTs = System.currentTimeMillis()
  
  def readResult = promise.future
  
  def begin() = sendReadRequests()
  
  def shutdown(): Unit = {}
  
  /** Sends a Read request to the specified store. Must be called from within a synchronized block */
  protected def sendReadRequest(dataStoreId: DataStoreID): Unit = clientMessenger.send( 
      Read(dataStoreId, clientMessenger.clientId, readUUID, objectPointer, readType))
      
  def receivedReplyFrom(storeId: DataStoreID): Boolean = synchronized {
    //storeStates.contains(storeId) || errors.contains(storeId)
    false
  }
      
  /** Sends a Read request to all stores that have not already responded. May be called outside a synchronized block */
  protected def sendReadRequests(): Unit = {
    logger.info(s"sending read requests for object ${objectPointer.uuid}.")
    synchronized { 
      retryCount += 1
      if (retryCount > 3)
        println(s"RESENDING READ REQUEST")
    }
    objectPointer.storePointers.foreach(sp => {
      val storeId = DataStoreID(objectPointer.poolUUID, sp.poolIndex)
      if (!receivedReplyFrom(storeId))
        sendReadRequest(storeId)
    })
  }
  
  def opportunisticRebuild(storeId: DataStoreID, ss: StoreState): Unit = readType match {
    case _: FullObject => restoredObject.foreach { t =>
      if (disableOpportunisticRebuild)
        return // skip if disabled
      
      val (objectState, currentUpdateSet) = t
        
      val repair = objectState match {
        case d: DataObjectState => d.timestamp > ss.timestamp && (d.revision != ss.revision || d.refcount != ss.refcount)
        
        case k: KeyValueObjectState => 
          if (k.revision != ss.revision || k.refcount != ss.refcount)
            true
          else
            ss.asInstanceOf[KVObjectStoreState].kvoss.updateSet != currentUpdateSet
          
        case m: MetadataObjectState => false
      }
      
      if (repair && objectState.lastUpdateTimestamp > ss.lastUpdateTimestamp && objectState.lastUpdateTimestamp - ss.lastUpdateTimestamp > opportunisticRebuildDelay) {
        logger.info(s"Sending Opportunistic Rebuild to store ${storeId.poolIndex} for object ${objectPointer.uuid}")
        
        val arrIdx = objectPointer.getEncodedDataIndexForStore(storeId).get
        val data = objectState.getRebuildDataForStore(storeId).get
        
        val oldUpdates = ss match {
          case _: DataObjectStoreState => Set[ObjectRevision]()
          case k: KVObjectStoreState => k.kvoss.updateSet
        }
        
        val msg = OpportunisticRebuild(storeId, clientMessenger.clientId, objectPointer, 
                    ss.revision, ss.refcount, oldUpdates,
                    objectState.revision, objectState.refcount, objectState.timestamp, data)
                    
        clientMessenger.send(msg)
      }
    }
    
    case _ =>
  }
  
  def receiveReadResponse(response:ReadResponse): Boolean = synchronized {
    
    def addError(err: ObjectReadError.Value): Unit = {
      //println(s"read error ${objectPointer.uuid}: $err from store ${response.fromStore}")
      errors += (response.fromStore -> err)
        
      if (storeStates.contains(response.fromStore))
        storeStates -= response.fromStore
    }
    
    response.result match {
      case Left(err) => addError(err)
          
      case Right(cs) => try {
        
        if (retryCount > 3) 
          logger.info(s"Read Response: ${response.fromStore.poolIndex} obj ${objectPointer.uuid} rev ${cs.revision}")
        
        // If any of the write locks reference transactions that are known to have successfully completed, the returned state from the store
        // is known to be out of date. Most likely, its just a race condition between the client and store detecting the transaction completion
        // to simplify handling, we'll just drop this message and immediately re-read. 
        ///
        if (cs.lockedWriteTransactions.forall(txuuid => getTransactionResult(txuuid).isEmpty)) {
          
          val ss = objectPointer match {
            case _: DataObjectPointer => new DataObjectStoreState(response.fromStore, cs.revision, cs.refcount, cs.timestamp, response.readTime, cs.sizeOnStore, cs.objectData)
            case _: KeyValueObjectPointer => new KVObjectStoreState(response.fromStore, cs.revision, cs.refcount, cs.timestamp, response.readTime, cs.objectData)
          }
          
          opportunisticRebuild(response.fromStore, ss)
          
          storeStates += (response.fromStore -> ss)
          
          if (errors.contains(response.fromStore))
            errors -= response.fromStore
        } else {
          // We know the read is out-of-date. Probably a race condition. Immediately re-issue a read
          // TODO - add some throttling/intelligent logic here
          storeStates -= response.fromStore
          sendReadRequest(response.fromStore)
        }
          
      } catch {
        // Encoding errors here will most likely be due to application-level bugs that corrupt the state of key-value objects. The stores
        // should guard against these kinds of errors but some insidious bug or another could cause this condition to be encountered.
        // TODO - Log a critical error. 
        case t: ObjectEncodingError => addError( ObjectReadError.CorruptedObject )
      }
    }
    
    if (!promise.isCompleted) {
      if (errors.size + storeStates.size >= objectPointer.ida.consistentRestoreThreshold && 
          errors.size >= objectPointer.ida.width - objectPointer.ida.restoreThreshold) {
        // We've received a consistentRestoreThreshold number of responses and enough of them are Fatal read errors that
        // there is no chance of ever succeeding. 
        val (invalidCount, corruptCount) = errors.values.foldLeft((0,0)) { (t, e) => e match {
          case ObjectReadError.ObjectMismatch      => (t._1+1, t._2)
          case ObjectReadError.InvalidLocalPointer => (t._1+1, t._2)
          case ObjectReadError.CorruptedObject     => (t._1, t._2+1)
        }}
        val err = if (invalidCount >= corruptCount) new InvalidObject(objectPointer) else new CorruptedObject(objectPointer)
        promise.success(Left(err))
      }
      else if (storeStates.size >= objectPointer.ida.consistentRestoreThreshold)
        checkComplete()
    }
    
    errors.size + storeStates.size == objectPointer.ida.width
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
    
    var opportunisticRebuildCandidates = Map[DataStoreID, StoreState]()
    
    val revisionCounts = storeStates.values.foldLeft(Map[ObjectRevision,Int]()) { (m, ss) =>
      m + (ss.revision -> (m.getOrElse(ss.revision, 0) + 1))
    }
    
    if (revisionCounts.size > 1) {
      val sortedCounts = revisionCounts.toList.sortBy( t => - t._2 )
      
      if (sortedCounts.head._2 == sortedCounts.tail.head._2) {
        return // No definitive winner. Wait for more responses
      }
      
      val pruneSet = sortedCounts.drop(1).map( t => t._1 ).toSet
      
      storeStates foreach { t =>
        
        val (storeId, ss) = t
        
        if (pruneSet.contains(ss.revision)) {
          storeStates -= storeId
          opportunisticRebuildCandidates += t
          sendReadRequest(storeId)
        }
      }
    }
    
    // At this point, only a single revision is left in the storeStates map.
    val restorable = objectPointer match {
      case _: DataObjectPointer => storeStates.size >= objectPointer.ida.consistentRestoreThreshold
      case _: KeyValueObjectPointer => 
        storeStates.size >= objectPointer.ida.consistentRestoreThreshold &&
        KeyValueObjectCodec.isRestorable(objectPointer.ida, storeStates.valuesIterator.map(ss => ss.asInstanceOf[KVObjectStoreState].kvoss).toList)
    }
    
    if (restorable) {
      // restore threshold reached. Decode object
      try {
        
        val objectState = objectPointer match {
          case op: DataObjectPointer     => restoreDataObject()
          case op: KeyValueObjectPointer => restoreKeyValueObject()
        }
        
        restoredObject = Some((objectState._1, objectState._2))
        
        HLCTimestamp.update(objectState._1.timestamp)
        
        opportunisticRebuildCandidates.foreach { t =>
          val (storeId, ss) = t
          opportunisticRebuild(storeId, ss)
        }
        
        promise.success(Right(objectState._1))
        
      } catch {
        case e: IDAError => promise.success(Left(new CorruptedIDA(objectPointer)))
        case e: ObjectEncodingError => promise.success(Left(new CorruptedContent(objectPointer)))
        case err: Throwable =>
          logger.error(s"Unexpected exception during object restoration: $err")
          com.ibm.aspen.util.printStack()
          promise.success(Left(new CorruptedObject(objectPointer)))
      }
    }
  }
  
  def getMetadata = {
    // The current refcount is the one with the highest updateSerial
    val refcount = storeStates.foldLeft(ObjectRefcount(-1,0))((ref, t) => if (t._2.refcount.updateSerial > ref.updateSerial) t._2.refcount else ref)
    val revision = storeStates.head._2.revision
    val timestamp = storeStates.head._2.timestamp
    
    val readTime = storeStates.foldLeft(HLCTimestamp.now)((mints, ss) => if (ss._2.readTimestamp < mints) ss._2.readTimestamp else mints)
    (revision, refcount, timestamp, readTime)
  }
  
  private def restoreDataObject() : (ObjectState, Set[ObjectRevision]) = {
    val (revision, refcount, timestamp, readTime) = getMetadata
    
    val sizeOnStore = storeStates.head._2.asInstanceOf[DataObjectStoreState].sizeOnStore
    
    val segments = storeStates.filter(t => ! t._2.asInstanceOf[DataObjectStoreState].objectData.isEmpty).foldLeft(List[(Byte, DataBuffer)]()) { (l, t) =>
      (t._1.poolIndex, t._2.asInstanceOf[DataObjectStoreState].objectData.get) :: l
    }
    
    try {
      // If something goes wrong with the IDA, it'll throw an IDAError exception
      val objectState = readType match {
        case _: MetadataOnly => MetadataObjectState(objectPointer, revision, refcount, timestamp, readTime)
        case _: FullObject => DataObjectState(objectPointer.asInstanceOf[DataObjectPointer], revision, refcount, timestamp, readTime, sizeOnStore, objectPointer.ida.restore(segments)) 
        case _: ByteRange => DataObjectState(objectPointer.asInstanceOf[DataObjectPointer], revision, refcount, timestamp, readTime, sizeOnStore, objectPointer.ida.restore(segments))
        case _ => throw new Exception("Invalid Read Type")
      }
      (objectState, Set())
    } catch {
      case t: IDAError => 
        logger.error(s"IDA ERROR Segments: $segments")
        throw t
    }

    
  }
  
  private def restoreKeyValueObject(): (ObjectState, Set[ObjectRevision]) = {
    val (revision, refcount, timestamp, readTime) = getMetadata

    val lkvoss = storeStates.map(t => (t._1.poolIndex.asInstanceOf[Int] -> t._2.asInstanceOf[KVObjectStoreState].kvoss)).toList
    
    def decode() = {
      
      val kvos = KeyValueObjectCodec.decode(objectPointer.asInstanceOf[KeyValueObjectPointer], revision, refcount, timestamp, readTime, lkvoss)
      
      (kvos, kvos.updateSet)
    }
    
    val objectState = readType match {
      case _: MetadataOnly       => (MetadataObjectState(objectPointer, revision, refcount, timestamp, readTime), Set[ObjectRevision]())
      case _: FullObject         => decode()
      case _: SingleKey          => decode()
      case _: LargestKeyLessThan => decode()
      case _: KeyRange           => decode()
      case _ => throw new Exception("Invalid Read Type")
    }
    
    (objectState._1, objectState._2)
  }
}

object BaseReadDriver {
  
  val DefaultOpportunisticRebuildDelay = Duration(5, SECONDS)
  
  abstract class StoreState(
      val storeId: DataStoreID,
      val revision: ObjectRevision,
      val refcount: ObjectRefcount,
      val timestamp: HLCTimestamp,
      val readTimestamp: HLCTimestamp) {
    
    def lastUpdateTimestamp: HLCTimestamp
  }
  
  class DataObjectStoreState(
      storeId: DataStoreID,
      revision: ObjectRevision,
      refcount: ObjectRefcount,
      timestamp: HLCTimestamp,
      readTimestamp: HLCTimestamp,
      val sizeOnStore: Int,
      val objectData: Option[DataBuffer]) extends StoreState(storeId, revision, refcount, timestamp, readTimestamp) {
    
    def lastUpdateTimestamp: HLCTimestamp = timestamp
  }
  
  class KVObjectStoreState(
      storeId: DataStoreID,
      revision: ObjectRevision,
      refcount: ObjectRefcount,
      timestamp: HLCTimestamp,
      readTimestamp: HLCTimestamp,
      objectData: Option[DataBuffer]) extends StoreState(storeId, revision, refcount, timestamp, readTimestamp) {
    
    val kvoss = objectData match {
      case None => new KeyValueObjectStoreState(None, None, None, None, Map())
      case Some(db) => KeyValueObjectStoreState(db)
    }
    
    def lastUpdateTimestamp: HLCTimestamp = kvoss.lastUpdateTimestamp
  }
      
  def noErrorRecoveryReadDriver(ec: ExecutionContext)(
      getTransactionResult: (UUID) => Option[Boolean],
      clientMessenger: ClientSideReadMessenger,
      objectPointer: ObjectPointer,
      readType: ReadType,
      retrieveLockedTransaction: Boolean,
      readUUID:UUID,
      disableOpportunisticRebuild: Boolean): ReadDriver = {
    new BaseReadDriver(getTransactionResult, clientMessenger, objectPointer, readType, retrieveLockedTransaction, readUUID)(ec)
  }
}