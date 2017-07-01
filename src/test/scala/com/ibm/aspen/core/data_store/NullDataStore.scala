package com.ibm.aspen.core.data_store

import scala.concurrent._

import com.ibm.aspen.core.transaction.TransactionDescription
import java.util.UUID
import com.ibm.aspen.core.transaction.LocalUpdateContent
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.allocation.AllocationError

/* A do-nothing store that simply returns empty successes/failures. Use this as a base class for 
 * mock stores used in tests. The "stored" objects have ObjectRevision(1,10)
 */
class NullDataStore(val storeId: DataStoreID) extends DataStore {
  
  import NullDataStore._
  
  def allocateNewObject(objectUUID: UUID, 
                        size: Option[Int], 
                        initialContent: Array[Byte],
                        initialRefcount: ObjectRefcount,
                        allocationTransactionUUID: UUID,
                        allocatingObject: ObjectPointer,
                        allocatingObjectRevision: ObjectRevision): Future[Either[AllocationError.Value, StorePointer]] = {
    Future.successful(Left(AllocationError.InsufficientSpace))
  }
  
  def getObject(storePointer: StorePointer): Future[Either[ObjectError.Value, (CurrentObjectState,Array[Byte])]] = {
    Future.successful(Left(ObjectError.InvalidLocalPointer))
  }
  
  def getCurrentObjectState(txd: TransactionDescription): Future[ Map[UUID, Either[ObjectError.Value, CurrentObjectState]] ] = {
    var m = Map[UUID, Either[ObjectError.Value, CurrentObjectState]]()
    txd.dataUpdates.foreach(du => m += (du.objectPointer.uuid -> Right(CurrentObjectState(du.objectPointer.uuid, revision, refcount, None))))
    txd.dataUpdates.foreach(ru => m += (ru.objectPointer.uuid -> Right(CurrentObjectState(ru.objectPointer.uuid, revision, refcount, None))))
    Future.successful(m)
  }
  
  def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, Either[ObjectError.Value, TransactionDescription]]] = None
  
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: LocalUpdateContent): Future[Unit] = Future.successful(())
  
  def discardTransaction(txd: TransactionDescription): Unit = ()
}

object NullDataStore {
  val revision = ObjectRevision(1,10)
  val refcount = ObjectRefcount(1,1)
}