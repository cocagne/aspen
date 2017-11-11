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
  
  def lockTransaction(txd: TransactionDescription): Future[List[ObjectTransactionError]] = synchronized {
    val requiredRevisions = txd.dataUpdates.map(du => (du.objectPointer.uuid -> du.requiredRevision)).toMap
    val requiredRefcounts = txd.refcountUpdates.map( ru => (ru.objectPointer.uuid -> ru.requiredRefcount)).toMap
    
    val localObjects = txd.allReferencedObjectsSet.foldLeft(List[(ObjectPointer, StorePointer)]())((l, op) => {
      if (op.poolUUID == storeId.poolUUID) {
        op.storePointers.find(_.poolIndex == storeId.poolIndex) match {
          case Some(sp) => (op, sp) :: l
          case None => l
        }
      } else
        l
    })
    
    val errors = localObjects.foldLeft(List[ObjectTransactionError]()) { (l, t) =>
      val (op, sp) = t
      getObject(sp.data) match {
        case None => new TransactionReadError(op, new InvalidLocalPointer) :: l
        case Some(obj) =>
          if (!requiredRevisions.contains(obj.uuid) && !requiredRefcounts.contains(obj.uuid))
            new TransactionReadError(op, ObjectMismatch()) :: l
          else if (requiredRevisions.contains(obj.uuid) && requiredRevisions(obj.uuid) != obj.revision)
            new RevisionMismatch(op, requiredRevisions(obj.uuid), obj.revision) :: l
          else if (requiredRefcounts.contains(obj.uuid) && requiredRefcounts(obj.uuid) != obj.refcount)
            new RefcountMismatch(op, requiredRefcounts(obj.uuid), obj.refcount) :: l
          else if (obj.lock.isDefined && obj.lock.get.transactionUUID != txd.transactionUUID)
            new TransactionCollision(op, obj.lock.get) :: l
          else
            l
      }
    }
    
    if (errors.isEmpty)
      localObjects.foreach(t => getObject(t._2.data).foreach(obj => obj.lock = Some(txd)))
    
    Future.successful(errors)
  }
  
  
  /** Commits the transaction changes and returns a Future to the completion of the commit operation.
   *  
   *  This method always returns Success() since there are no recovery steps the transaction logic can take for failures
   *  that occur after the commit decision has been made. 
   */
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[Array[ByteBuffer]]): Future[Unit] = synchronized {
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
        assert(localUpdates.isDefined, "Attempted to commit data update without having data update content!")
        assert(localUpdates.get.size > idx, "Attempted to commit with data update index greater than size of content array!")
        if (du.requiredRevision == obj.revision) {
          obj.revision = du.operation match {
            case DataUpdateOperation.Append => 
              val appendData = localUpdates.get.apply(idx)
              val newData = ByteBuffer.allocateDirect(obj.data.capacity + appendData.capacity)

              newData.put(obj.data)
              newData.put(appendData)
              newData.position(0)
              
              obj.data = newData
              ObjectRevision(obj.revision.overwriteCount, obj.data.capacity)
              
            case DataUpdateOperation.Overwrite => 
              obj.data = localUpdates.get.apply(idx)
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
      var lastCommittedTxUUID: UUID,
      var lock: Option[TransactionDescription])
}