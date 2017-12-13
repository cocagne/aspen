package com.ibm.aspen.core.data_store

import scala.concurrent._

import com.ibm.aspen.core.transaction.TransactionDescription
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.allocation.AllocationErrors
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.HLCTimestamp

/* A do-nothing store that simply returns empty successes/failures. Use this as a base class for 
 * mock stores used in tests. The "stored" objects have ObjectRevision(1,10)
 */
class NullDataStore(val storeId: DataStoreID) extends DataStore {
  
  import NullDataStore._
  
  def initialized: Future[DataStore] = Future.successful(this)
  
  def close(): Future[Unit] = Future.successful(())
  
  def allocate(newObjects: List[Allocate.NewObject],
               timestamp: HLCTimestamp,
               allocationTransactionUUID: UUID,
               allocatingObject: ObjectPointer,
               allocatingObjectRevision: ObjectRevision): Future[Either[AllocationErrors.Value, AllocationRecoveryState]] = {
    Future.successful(Left(AllocationErrors.InsufficientSpace))
  }
  
  def allocationResolved(ars: AllocationRecoveryState, committed: Boolean): Future[Unit] = Future.successful(())
  
  def allocationRecoveryComplete(ars: AllocationRecoveryState, commit: Map[UUID, Boolean]): Future[Unit] = Future.successful(())
  
  def getObject(objectPointer: ObjectPointer, storePointer: StorePointer): Future[Either[ObjectReadError, (CurrentObjectState,DataBuffer)]] = {
    Future.successful(Left(new InvalidLocalPointer))
  }
  
  def lockTransaction(txd: TransactionDescription, updateData: Option[List[LocalUpdate]]): Future[List[ObjectTransactionError]] = Future.successful(Nil)
  
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[List[LocalUpdate]]): Future[Unit] = Future.successful(())
  
  def discardTransaction(txd: TransactionDescription): Unit = ()
}

object NullDataStore {
  val revision = ObjectRevision(1,10)
  val refcount = ObjectRefcount(1,1)
}