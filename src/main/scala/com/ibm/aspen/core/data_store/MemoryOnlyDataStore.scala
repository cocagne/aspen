package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.LocalUpdateContent
import com.ibm.aspen.core.transaction.DataUpdateOperation
import scala.concurrent.Future
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.allocation.AllocationError

// TODO: Use separate locks for DataUpdates and RefcountUpdates. This would allow them to not conflict

class MemoryOnlyDataStore(
    override val storeId: DataStoreID) extends DataStore {
  
  import MemoryOnlyDataStore._
  
  private [this] var objects:Map[Int, Object] = Map()
  private [this] var nextLocalPointerId = 1
  
  private def nextLocalPointer() = {
    val lp = nextLocalPointerId
    nextLocalPointerId += 1
    (lp, ByteBuffer.allocate(4).putInt(lp).array())
  }
  
  private def getObject(ba: Array[Byte]) = objects.get(ByteBuffer.wrap(ba).getInt)
  
  /** Allocates a new Object on the store */
  def allocateNewObject(objectUUID: UUID, 
                        size: Option[Int], 
                        initialContent: ByteBuffer,
                        initialRefcount: ObjectRefcount,
                        allocationTransactionUUID: UUID,
                        allocatingObject: ObjectPointer,
                        allocatingObjectRevision: ObjectRevision): Future[Either[AllocationError.Value, StorePointer]] = synchronized {
    val (objId, lpArray) = nextLocalPointer()
    
    objects += (objId -> new Object(objectUUID, ObjectRevision(0, initialContent.capacity), initialRefcount, initialContent, None))
    
    Future.successful(Right(StorePointer(storeId.poolIndex, lpArray)))
  }
  
  /** Reads an object on the store */
  def getObject(storePointer: StorePointer): Future[Either[ObjectError.Value, (CurrentObjectState,ByteBuffer)]] = synchronized {
    if (storePointer.poolIndex != storeId.poolIndex || storePointer.data.length != 4)
      return Future.successful(Left(ObjectError.InvalidLocalPointer))
    
    getObject(storePointer.data) match {
      case None => Future.successful(Left(ObjectError.InvalidLocalPointer))
      case Some(obj) =>
        val cstate = CurrentObjectState(obj.uuid, obj.revision, obj.refcount, obj.lock)
        Future.successful(Right( (cstate, obj.data) ))
    }
  }
  
  /** Returns a future to a map of the current object state for all hosted objects referenced by the TransactionDescription
   *  
   *  This method always returns a Success(). Any errors encountered along the way are noted within the CurrentObjectState
   *  associated with the object(s) for which errors were encountered. 
   */
  def getCurrentObjectState(txd: TransactionDescription): Future[ Map[UUID, Either[ObjectError.Value, CurrentObjectState]] ] = {
    
    val m = synchronized {
      for {
        op <- txd.allReferencedObjectsSet if op.poolUUID == storeId.poolUUID
         
        spo = op.storePointers.find(_.poolIndex == storeId.poolIndex)
        if spo.isDefined
        
        result: Either[ObjectError.Value, CurrentObjectState] = getObject(spo.get.data) match {
          case None => Left(ObjectError.InvalidLocalPointer)
          case Some(obj) => if (op.uuid == obj.uuid) 
            Right(CurrentObjectState(obj.uuid, obj.revision, obj.refcount, obj.lock))
          else  
            Left(ObjectError.ObjectMismatch)
        }
      } yield (op.uuid -> result)
    }
    
    Future.successful(m.toMap)
  }
  
  /** Locks all objects referenced by the transaction or returns a map of collisions and/or errors */
  def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, Either[ObjectError.Value, TransactionDescription]]] = synchronized {
    var localObjects = Set[Object]()
    
    val requiredRevisions = txd.dataUpdates.map(du => (du.objectPointer.uuid -> du.requiredRevision)).toMap
    val requiredRefcounts = txd.refcountUpdates.map( ru => (ru.objectPointer.uuid -> ru.requiredRefcount)).toMap
    
    val errs = for {
      op <- txd.allReferencedObjectsSet if op.poolUUID == storeId.poolUUID
      spo = op.storePointers.find(_.poolIndex == storeId.poolIndex)
      if spo.isDefined
      
      result: Option[Either[ObjectError.Value, TransactionDescription]] = getObject(spo.get.data) match {
        case None => Some(Left(ObjectError.InvalidLocalPointer))
        case Some(obj) =>
          if (op.uuid == obj.uuid) 
            obj.lock match {
            case None => 
              val revision_ok = requiredRevisions.get(obj.uuid) match {
                case Some(required) => obj.revision == required
                case None => true
              }
              val refcount_ok = requiredRefcounts.get(obj.uuid) match {
                case Some(required) => obj.refcount == required
                case None => true
              }
              if (revision_ok && refcount_ok) {
                localObjects += obj
                None
              } else {
                if (!revision_ok)
                  Some(Left(ObjectError.RevisionMismatch))
                else
                  Some(Left(ObjectError.RefcountMismatch))
              }
            case Some(txd) => Some(Right(txd))
          }
          else  
            Some(Left(ObjectError.ObjectMismatch))
      }
      if result.isDefined
    } yield (op.uuid -> result.get)
    
    if (errs.isEmpty) {
      val lock = Some(txd)
      localObjects.foreach(_.lock = lock)
      None
    } else {
      Some(errs.toMap)
    }
  }
  
  
  /** Commits the transaction changes and returns a Future to the completion of the commit operation.
   *  
   *  This method always returns Success() since there are no recovery steps the transaction logic can take for failures
   *  that occur after the commit decision has been made. 
   */
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: LocalUpdateContent): Future[Unit] = synchronized {
    val localSet = for {
      op <- txd.allReferencedObjectsSet if op.poolUUID == storeId.poolUUID
      spo = op.storePointers.find(_.poolIndex == storeId.poolIndex)
      if spo.isDefined
      
      obj = getObject(spo.get.data) 
      
      if obj.isDefined
    } yield (op.uuid -> obj.get)
    
    val localObjects = localSet.toMap
    
    // Iterate over all DataUpdates & RefcountUpdates and apply operations if and only if the required revision/refcount still matches
    
    for ((du, idx) <- txd.dataUpdates.zipWithIndex) {
      localObjects.get(du.objectPointer.uuid).foreach(obj => {
        if (du.requiredRevision == obj.revision) {
          obj.revision = du.operation match {
            case DataUpdateOperation.Append => 
              val appendData = localUpdates.getDataForUpdateIndex(idx)
              val newData = ByteBuffer.allocateDirect(obj.data.capacity + appendData.capacity)

              newData.put(obj.data)
              newData.put(appendData)
              newData.position(0)
              
              obj.data = newData
              ObjectRevision(obj.revision.overwriteCount, obj.data.capacity)
              
            case DataUpdateOperation.Overwrite => 
              obj.data = localUpdates.getDataForUpdateIndex(idx)
              ObjectRevision(obj.revision.overwriteCount+1, obj.data.capacity)
          }
        }
      })  
    }
    
    for ((ru, idx) <- txd.refcountUpdates.zipWithIndex) {
      localObjects.get(ru.objectPointer.uuid).foreach(obj => {
        if (ru.requiredRefcount == obj.refcount)
          obj.refcount = ru.newRefcount
      })  
    }
    
    // It is possible for transactions to commit even if the objects are currently locked to some other
    // transaction. Only clear locks with matching transaction UUIDs.
    for (t <- localSet) {
      t._2.lock.foreach( lockedTxd => if (lockedTxd.transactionUUID == txd.transactionUUID) t._2.lock = None )
    }
    
    Future.successful(())
  }
  
  /** Called at the end of each transaction to ensure all object locks are released.
   *  
   *  For successful transactions, commitTransactionUpdates will be called first and it should release the
   *  locks while the finalization actions run. Both committed and aborted transactions call this method.
   * 
   */
  def discardTransaction(txd: TransactionDescription): Unit = synchronized {
    for {
      op <- txd.allReferencedObjectsSet if op.poolUUID == storeId.poolUUID
      spo = op.storePointers.find(_.poolIndex == storeId.poolIndex)
      if spo.isDefined
      
      optObj = getObject(spo.get.data)
    }{
      optObj.foreach(obj => obj.lock.foreach(lockedTxd => if (lockedTxd.transactionUUID == txd.transactionUUID) obj.lock = None))
    }
  }
}

object MemoryOnlyDataStore {
  private class Object(
      val uuid: UUID, 
      var revision:ObjectRevision, 
      var refcount: ObjectRefcount, 
      var data: ByteBuffer, 
      var lock: Option[TransactionDescription])
}