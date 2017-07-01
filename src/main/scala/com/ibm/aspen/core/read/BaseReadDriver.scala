package com.ibm.aspen.core.read

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.ClientSideReadMessenger
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.CurrentObjectState
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.objects.StorePointer

class BaseReadDriver(
    val objectPointer: ObjectPointer,
    val retrieveObjectData: Boolean,
    val retrieveLockedTransaction: Boolean,
    val clientMessenger: ClientSideReadMessenger, 
    val readUUID:UUID)(implicit ec: ExecutionContext) extends ReadDriver {
  
  import BaseReadDriver._
  
  protected var responses = Map[DataStoreID, Either[ReadError.Value, StoreState]]()
  protected val promise = Promise[Either[ReadError, ObjectState]]
  
  def readResult = promise.future
  
  protected def sendReadRequest(dataStoreId: DataStoreID): Unit = clientMessenger.send(dataStoreId, 
      Read(dataStoreId, clientMessenger.client, readUUID, objectPointer, retrieveObjectData, retrieveLockedTransaction))
      
  protected def sendReadRequests(): Unit = {
    val heardFrom = synchronized { responses.keySet }
    objectPointer.storePointers.foreach(sp => {
      val storeId = DataStoreID(objectPointer.poolUUID, sp.poolIndex)
      if (!heardFrom.contains(storeId))
        sendReadRequest(storeId)
    })
  }
  
  protected def complete(): Unit = {
    
    val segments = objectPointer.storePointers.foldLeft(List[(Byte,Option[Array[Byte]])]())( (l, sp) => {
      responses.get(DataStoreID(objectPointer.poolUUID, sp.poolIndex)) match {
        case None => (sp.poolIndex, None) :: l
        case Some(either) => either match {
          case Left(_) => (sp.poolIndex, None) :: l
          case Right(ss) => (sp.poolIndex, ss.objectData) :: l
        }
      }
    })
    
    val odata = if (retrieveObjectData) Some(objectPointer.ida.restore(segments)) else None
      
    val zv = ObjectRevision(0,0)
    val zr = ObjectRefcount(0,0)
    val zl = List[(DataStoreID, TransactionDescription)]()
    
    val (highestRevision, highestRefcount, lockedTransactions) = responses.foldLeft((zv,zr,zl))( (h, t) => t._2 match {
      case Left(_) => h
      case Right(ss) =>
        val hrev = if (ss.revision > h._1) ss.revision else h._1
        val href = if (ss.refcount.updateSerial > h._2.updateSerial) ss.refcount else h._2 
        val locks = ss.lockedTransaction match {
          case None => h._3
          case Some(txd) => (ss.storeId, txd) :: h._3
        }
        (hrev, href, locks)
    })
    
    val objState = ObjectState(objectPointer, highestRevision, highestRefcount, odata, 
                               if (retrieveLockedTransaction) Some(lockedTransactions) else None )
                                      
    promise.success(Right(objState))
  }
  
  
  def receiveReadResponse(response:ReadResponse): Unit = synchronized {
    if (promise.isCompleted)
      return // Already done
      
    val r = response.result match {
      case Left(err) => Left(err)
      case Right(cs) => Right(StoreState(response.fromStore, cs.revision, cs.refcount, cs.objectData, cs.lockedTransaction))
    }
    
    responses += (response.fromStore -> r)
    
    if (responses.size < objectPointer.ida.consistentRestoreThreshold)
      return // Definitely can't take any action yet
    
    // Find the highest revision then filter out all responses for old revisions
    responses.values.reduceLeft((a,b) => (a,b) match {
      case (Left(e1),  Left(e2))  => Left(e1)
      case (Left(e),   Right(s))  => Right(s)
      case (Right(s),  Left(e))   => Right(s)
      case (Right(s1), Right(s2)) => Right(if (s1.revision > s2.revision) s1 else s2) 
    }) match {
      case Left(_) =>
      case Right(highest) => responses = responses.filter( t => t._2 match {
        case Left(_) => true
        case Right(s) => if (s.revision == highest.revision) 
          true 
        else {
          // We read old state. Discard it and read the current state
          sendReadRequest(s.storeId)
          false
        }
      })
    }
    
    val (numErrs, numOkay) = responses.foldLeft((0,0))((counts, t) => t._2 match {
      case Left(_) => (counts._1+1, counts._2)
      case Right(_) => (counts._1, counts._2+1)
    })
    
    if (numOkay >= objectPointer.ida.consistentRestoreThreshold) {
      try complete() catch {
        case e: IDAError => 
          promise.success(Left(e))
      }
    } else if ( numErrs > objectPointer.ida.width - objectPointer.ida.restoreThreshold ) {
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

object BaseReadDriver {
  case class StoreState(
      storeId: DataStoreID,
      revision: ObjectRevision,
      refcount: ObjectRefcount,
      objectData: Option[Array[Byte]],
      lockedTransaction: Option[TransactionDescription])
}