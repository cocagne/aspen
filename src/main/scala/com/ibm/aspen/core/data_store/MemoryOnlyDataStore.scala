package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.DataUpdateOperation
import scala.concurrent.Future
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.allocation.AllocationErrors
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.transaction.DataUpdate
import com.ibm.aspen.core.transaction.RefcountUpdate

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
  
  def initialize(transactionRecoveryStates: List[TransactionRecoveryState]): Future[Unit] = Future.successful(())
  
  def close(): Future[Unit] = Future.successful(())
  
  private def getObject(ba: Array[Byte]): Option[Object] = objects.get(ByteBuffer.wrap(ba).getInt)
  
  /** Allocates a new Object on the store */
  def allocateNewObject(objectUUID: UUID, 
                        size: Option[Int], 
                        initialContent: ByteBuffer,
                        initialRefcount: ObjectRefcount,
                        allocationTransactionUUID: UUID,
                        allocatingObject: ObjectPointer,
                        allocatingObjectRevision: ObjectRevision): Future[Either[AllocationErrors.Value, StorePointer]] = synchronized {
    val (objId, lpArray) = nextLocalPointer()
    
    objects += (objId -> new Object(objectUUID, ObjectRevision(0, initialContent.capacity), initialRefcount, initialContent, allocationTransactionUUID, None))
    
    Future.successful(Right(StorePointer(storeId.poolIndex, lpArray)))
  }
  
  /** Reads an object on the store */
  def getObject(objectPointer: ObjectPointer, storePointer: StorePointer): Future[Either[ObjectReadError, (CurrentObjectState,ByteBuffer)]] = synchronized {
    if (storePointer.poolIndex != storeId.poolIndex || storePointer.data.length != 4)
      return Future.successful(Left(new InvalidLocalPointer))
    
    getObject(storePointer.data) match {
      case None => Future.successful(Left(new InvalidLocalPointer))
      case Some(obj) =>
        val r = if (obj.uuid != objectPointer.uuid)
          Left(ObjectMismatch())
        else
          Right((CurrentObjectState(obj.uuid, obj.revision, obj.refcount, obj.lastCommittedTxUUID, obj.lock), obj.data))
        
        Future.successful(r)
    }
  }
  
  def lockTransaction(txd: TransactionDescription, updateData: Option[List[LocalUpdate]]): Future[List[ObjectTransactionError]] = synchronized {
    val checker = new TransactionErrorChecker(txd, updateData)
    
    def getCurrentState(op: ObjectPointer, sp:StorePointer): Either[ObjectReadError, (ObjectRevision, ObjectRefcount, Option[TransactionDescription])] = {
      getObject(sp.data) match {
        case None => Left(new InvalidLocalPointer)
        case Some(obj) => Right((obj.revision, obj.refcount, obj.lock))
      }
    }
    
    val errors = checker.getErrors(getCurrentState)
    
    if (errors.isEmpty)
      checker.localObjects.foreach(t => getObject(t._2.data).foreach(obj => obj.lock = Some(txd)))
    
    Future.successful(errors)
  }
  
  /** Commits the transaction changes and returns a Future to the completion of the commit operation.
   *  
   *  This method always returns Success() since there are no recovery steps the transaction logic can take for failures
   *  that occur after the commit decision has been made. 
   */
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[List[LocalUpdate]]): Future[Unit] = synchronized {
    val localSet = for {
      op <- txd.allReferencedObjectsSet if op.poolUUID == storeId.poolUUID
      spo = op.storePointers.find(_.poolIndex == storeId.poolIndex)
      if spo.isDefined
      
      obj = getObject(spo.get.data) 
      
      if obj.isDefined
    } yield (op.uuid -> obj.get)
    
    val localObjects = localSet.toMap
    
    val objectUpdates = localUpdates match {
      case None => Map[UUID, ByteBuffer]()
      case Some(lst) => lst.map(lu => (lu.objectUUID -> lu.data)).toMap
    }
    
    // Iterate over all DataUpdates & RefcountUpdates and apply operations if and only if the required revision/refcount still matches
    txd.requirements.foreach { r =>
      localObjects.get(r.objectPointer.uuid).foreach { obj =>
        r match {
          case du: DataUpdate =>
            objectUpdates.get(r.objectPointer.uuid).foreach { data =>
              du.operation match {
                case DataUpdateOperation.Append => 
                  if (obj.revision == du.requiredRevision) {
                    // Unlike overwrite which sets the full state of the object, appends can only be applied if
                    // our current state matches the expected value. We can safely ignore this commit since we
                    // cannot have voted for it to complete. The catch-up process will repair the object.
                    val newData = ByteBuffer.allocateDirect(obj.data.capacity + (data.limit() - data.position()))
      
                    newData.put(obj.data)
                    newData.put(data.asReadOnlyBuffer())
                    newData.position(0)
                    
                    obj.data = newData
                    obj.revision = obj.revision.append(obj.data.capacity)
                  }
                  
                case DataUpdateOperation.Overwrite =>
                  obj.data = data.asReadOnlyBuffer()
                  obj.revision = obj.revision.overwrite(obj.data.limit() - obj.data.position())
              }
            }
            
          case ru: RefcountUpdate =>
            obj.refcount = ru.newRefcount
        }
      }
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
      var lastCommittedTxUUID: UUID,
      var lock: Option[TransactionDescription])
}