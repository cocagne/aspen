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

/* A do-nothing store that simply returns empty successes/failures. Use this as a base class for 
 * mock stores used in tests. The "stored" objects have ObjectRevision(1,10)
 */
class NullDataStore(val storeId: DataStoreID) extends DataStore {
  
  import NullDataStore._
  
  def initialize(transactionRecoveryStates: List[TransactionRecoveryState]): Future[Unit] = Future.successful(())
  
  def close(): Future[Unit] = Future.successful(())
  
  def allocateNewObject(objectUUID: UUID, 
                        size: Option[Int], 
                        initialContent: ByteBuffer,
                        initialRefcount: ObjectRefcount,
                        allocationTransactionUUID: UUID,
                        allocatingObject: ObjectPointer,
                        allocatingObjectRevision: ObjectRevision): Future[Either[AllocationErrors.Value, StorePointer]] = {
    Future.successful(Left(AllocationErrors.InsufficientSpace))
  }
  
  def getObject(objectPointer: ObjectPointer, storePointer: StorePointer): Future[Either[ObjectReadError, (CurrentObjectState,ByteBuffer)]] = {
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