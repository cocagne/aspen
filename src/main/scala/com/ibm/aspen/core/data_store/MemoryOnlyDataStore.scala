package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.LocalUpdateContent
import scala.concurrent.Future
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import java.nio.ByteBuffer

class MemoryOnlyDataStore(
    override val storeId: DataStoreID) extends DataStore {
  
  import MemoryOnlyDataStore._
  /*
   case class CurrentObjectState(
    uuid: UUID,
    revision: ObjectRevision,
    refcount: ObjectRefcount)
    
    object ObjectError extends Enumeration {
      
      /** Supplied LocalPointer is not understood by this store or points to an invalid location */
      val InvalidLocalPointer = Value("InvalidLocalPointer")
      
      /** The local pointer is valid but the UUID of the stored object (if any) does not match that of the read request.
       *  
       *  This can occur if the original object is deleted and its storage location has been reassigned to a
       *  new object.
       */
      val ObjectMismatch = Value("ObjectMismatch")
      
      /** Checksum stored with the object does not match the read content. 
       *  
       *  This should be due to media errors 
       */
      val CorruptedObject = Value("CorruptedObject")
      
}
   */
  
  private [this] var objects:Map[Int, Object] = Map()
  private [this] var nextLocalPointerId = 1
  
  private def nextLocalPointer = {
    val lp = nextLocalPointerId
    nextLocalPointerId += 1
    ByteBuffer.allocate(4).putInt(lp).array()
  }
  
  private def getObject(ba: Array[Byte]) = objects.get(ByteBuffer.wrap(ba).getInt)
  
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
            Right(CurrentObjectState(obj.uuid, obj.revision, obj.refcount))
          else  
            Left(ObjectError.ObjectMismatch)
        }
      } yield (op.uuid -> result)
    }
    
    Future.successful(m.toMap)
  }
  
  /** Locks all objects referenced by the transaction or returns a map of collisions */
  def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, Either[ObjectError.Value, TransactionDescription]]] = synchronized {
    val errs = for {
      op <- txd.allReferencedObjectsSet if op.poolUUID == storeId.poolUUID
      spo = op.storePointers.find(_.poolIndex == storeId.poolIndex)
      if spo.isDefined
      
      result: Option[Either[ObjectError.Value, TransactionDescription]] = getObject(spo.get.data) match {
        case None => Some(Left(ObjectError.InvalidLocalPointer))
        case Some(obj) => if (op.uuid == obj.uuid) 
          obj.lock.map(Right(_))
        else  
          Some(Left(ObjectError.ObjectMismatch))
      }
      if result.isDefined
    } yield (op.uuid -> result.get)
    
    if (errs.isEmpty)
      None
    else
      Some(errs.toMap)
  }
  
  
  /** Commits the transaction changes and returns a Future to the completion of the commit operation.
   *  
   *  This method always returns Success() since there are no recovery steps the transaction logic can take for failures
   *  that occur after the commit decision has been made. 
   */
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: LocalUpdateContent): Future[Unit] = Future.successful(())
  
  /** Called at the end of each transaction to ensure all object locks are released.
   *  
   *  For successful transactions, commitTransactionUpdates will be called first and it should release the
   *  locks while the finalization actions run. Both committed and aborted transactions call this method.
   * 
   */
  def discardTransaction(txd: TransactionDescription): Unit = ()
}

object MemoryOnlyDataStore {
  private class Object(
      val uuid: UUID, 
      var revision:ObjectRevision, 
      var refcount: ObjectRefcount, 
      var data: Array[Byte], 
      var lock: Option[TransactionDescription])
}