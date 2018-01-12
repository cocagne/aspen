package com.ibm.aspen.core.read

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.ClientSideReadMessenger
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.StoreObjectState
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.objects.StorePointer
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp

class BaseReadDriver(
    val clientMessenger: ClientSideReadMessenger,
    val objectPointer: ObjectPointer,
    val retrieveObjectData: Boolean,
    val retrieveLockedTransaction: Boolean, 
    val readUUID:UUID)(implicit ec: ExecutionContext) extends ReadDriver {
  
  import BaseReadDriver._
  
  protected var responses = Map[DataStoreID, Either[ReadError.Value, StoreState]]()
  protected val promise = Promise[Either[ReadError, ObjectReadState]]
  
  def readResult = promise.future
  
  def begin() = sendReadRequests()
  
  /** Sends a Read request to the specified store. Must be called from within a synchronized block */
  protected def sendReadRequest(dataStoreId: DataStoreID): Unit = clientMessenger.send(dataStoreId, 
      Read(dataStoreId, clientMessenger.clientId, readUUID, objectPointer, retrieveObjectData, retrieveLockedTransaction))
      
  /** Sends a Read request to all stores that have not already responded. May be called outside a synchronized block */
  protected def sendReadRequests(): Unit = {
    val heardFrom = synchronized { responses.keySet }
    objectPointer.storePointers.foreach(sp => {
      val storeId = DataStoreID(objectPointer.poolUUID, sp.poolIndex)
      if (!heardFrom.contains(storeId))
        sendReadRequest(storeId)
    })
  }
  
  /** Successfully complete the read operation or throw and IDAError. Must be called from within a synchronized block */
  protected def complete(): Unit = {
    
    val segments = objectPointer.storePointers.foldLeft(List[(Byte,Option[DataBuffer])]())( (l, sp) => {
      responses.get(DataStoreID(objectPointer.poolUUID, sp.poolIndex)) match {
        case None => (sp.poolIndex, None) :: l
        case Some(either) => either match {
          case Left(_) => (sp.poolIndex, None) :: l
          case Right(ss) => (sp.poolIndex, ss.objectData) :: l
        }
      }
    })
    
    // If something goes wrong with the IDA, it'll throw an IDAError exception
    val odata = if (retrieveObjectData) Some(objectPointer.ida.restore(segments)) else None
      
    val zv = ObjectRevision(new UUID(0,0))
    val zr = ObjectRefcount(0,0)
    val zt = HLCTimestamp(0) 
    val zl = List[(DataStoreID, TransactionDescription)]()
    
    
    val (highestRevision, highestRefcount, highestTimstamp, lockedTransactions) = responses.foldLeft((zv,zr,zt,zl))( (h, t) => t._2 match {
      case Left(_) => h
      case Right(ss) =>
        val hrev = if (ss.timestamp.compareTo(h._3) > 0) ss.revision else h._1
        val href = if (ss.refcount.updateSerial > h._2.updateSerial) ss.refcount else h._2 
        val hts  = if (ss.timestamp.compareTo(h._3) > 0) ss.timestamp else h._3
        val locks = ss.lockedTransaction match {
          case None => h._4
          case Some(txd) => (ss.storeId, txd) :: h._4
        }
        
        (hrev, href, hts, locks)
    })
    
    val objState = ObjectReadState(objectPointer, highestRevision, highestRefcount, highestTimstamp, odata, 
                               if (retrieveLockedTransaction) Some(lockedTransactions) else None )
                                      
    promise.success(Right(objState))
  }
  
  
  def receiveReadResponse(response:ReadResponse): Unit = synchronized {
    if (promise.isCompleted)
      return // Already done
      
    val r = response.result match {
      case Left(err) => Left(err)
      case Right(cs) => Right(StoreState(response.fromStore, cs.revision, cs.refcount, cs.timestamp, cs.objectData, cs.lockedTransaction))
    }
    
    responses += (response.fromStore -> r)
    
    if (responses.size < objectPointer.ida.consistentRestoreThreshold)
      return // Definitely can't take any action yet
    
    val revisionCounts = responses.values.foldLeft(Map[UUID,Int]()){ 
      (m, e) => e match {
        case Left(err) => m
        case Right(ss) => m + (ss.revision.lastUpdateTxUUID -> (m.getOrElse(ss.revision.lastUpdateTxUUID, 0)+1)) 
      }
    }
    
    val start: Option[(UUID, Int)] = None
    
    // Determine which revision has received the most responses and the number of those responses.
    // If two or more revisions are tied in response counts, we can't figure out which to prune
    val oMostRepliedRevision = revisionCounts.foldLeft((start,start)){ (o, t) => o match {
      case (None, None) => (Some(t), None)
      case (Some(highest), None) => if (t._2 > highest._2) (Some(t), Some(highest)) else (Some(highest), Some(t))
      case (Some(highest), Some(nextHighest)) =>
        if (t._2 > highest._2)
          (Some(t), Some(highest))
        else if (t._2 > nextHighest._2)
          (Some(highest), Some(t))
        else
          (Some(highest), Some(nextHighest))
      case (None, Some(nextHighest)) => o // Can't get here 
    }} match {
      case (None, None) => None
      case (Some(highest), None) => Some(highest)
      case (Some(highest), Some(nextHighest)) => if (highest._2 == nextHighest._2) None else  Some(highest)
      case (None, Some(x)) => None // can't get here
    }
    
    oMostRepliedRevision foreach { mostRepliedRevision =>
      val revision = ObjectRevision(mostRepliedRevision._1)
      val count = mostRepliedRevision._2
      val thresholdReached = count >= objectPointer.ida.consistentRestoreThreshold
      
      // Filter out revisions that don't match the one with the most responses and request a re-read from all
      // stores from which we didn't receive that revision
      responses = responses.filter( t => t._2 match {
        case Left(_) => true
        case Right(s) => if (s.revision == revision) 
          true 
        else {
          // We read old state. Discard it and read the current state
          if (!thresholdReached)
            sendReadRequest(s.storeId)
          false
        }
      })
      
      if (thresholdReached) {
        try {
          complete() 
        } catch {
          case e: IDAError => 
            promise.success(Left(e))
        }
      }else {
        val numErrs = responses.foldLeft(0)((errCount, t) => t._2 match {
          case Left(_) => errCount + 1
          case Right(_) => errCount
        })
        
        if ( numErrs >= objectPointer.ida.width - objectPointer.ida.restoreThreshold ) {
          val errMap = objectPointer.storePointers.foldLeft(Map[DataStoreID,Option[ReadError.Value]]())( (m, sp) => {
            val storeId = DataStoreID(objectPointer.poolUUID, sp.poolIndex) 
            responses.get(storeId) match {
              case None => m + (storeId -> Some(ReadError.NoResponse))
              case Some(either) => either match {
                case Left(err) => m + (storeId -> Some(err))
                case Right(ss) => m + (storeId -> None)
              }
            }
          })
          promise.success(Left(new ThresholdError(errMap)))
        }
      }
    }
  }
}

object BaseReadDriver {
  case class StoreState(
      storeId: DataStoreID,
      revision: ObjectRevision,
      refcount: ObjectRefcount,
      timestamp: HLCTimestamp,
      objectData: Option[DataBuffer],
      lockedTransaction: Option[TransactionDescription])
      
  def noErrorRecoveryReadDriver(ec: ExecutionContext)(
      clientMessenger: ClientSideReadMessenger,
      objectPointer: ObjectPointer,
      retrieveObjectData: Boolean,
      retrieveLockedTransaction: Boolean,
      readUUID:UUID): ReadDriver = {
    new BaseReadDriver(clientMessenger, objectPointer, retrieveObjectData, retrieveLockedTransaction, readUUID)(ec)
  }
}