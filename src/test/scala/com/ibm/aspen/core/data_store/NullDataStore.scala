package com.ibm.aspen.core.data_store

import scala.concurrent._
import com.ibm.aspen.core.transaction.{LocalUpdate, PreTransactionOpportunisticRebuild, TransactionDescription, TransactionRecoveryState}
import java.util.UUID

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.allocation._
import java.nio.ByteBuffer

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.read.OpportunisticRebuild

/* A do-nothing store that simply returns empty successes/failures. Use this as a base class for 
 * mock stores used in tests. The "stored" objects have ObjectRevision(1,10)
 */
class NullDataStore(val storeId: DataStoreID) extends DataStore {

  import NullDataStore._
  
  override val executionContext: ExecutionContext = ExecutionContext.global
  
  val initialized: Future[DataStore] = Future.successful(this)

  def logTransactionStatus(log: String => Unit): Unit = ()

  def allTransactionsComplete: Boolean = true

  def transactionInProgress(transactionUUID: UUID): Boolean = false

  def close(): Future[Unit] = Future.successful(())
  
  def bootstrapAllocateNewObject(objectUUID: UUID, initialContent: DataBuffer, timestamp: HLCTimestamp): Future[StorePointer] = Future.successful(new StorePointer(0, new Array[Byte](0)))
  
  
  def bootstrapOverwriteObject(objectPointer: ObjectPointer, newContent: DataBuffer, timestamp: HLCTimestamp): Future[Unit] = Future.successful(())

  def allocate(newObjectUUID: UUID,
               options: AllocationOptions,
               objectSize: Option[Int],
               initialRefcount: ObjectRefcount,
               objectData: DataBuffer,
               timestamp: HLCTimestamp,
               allocationTransactionUUID: UUID,
               revisionGuard: AllocationRevisionGuard): Future[Either[AllocationErrors.Value, AllocationRecoveryState]] = {
    Future.successful(Left(AllocationErrors.InsufficientSpace))
  }
  
  def allocationResolved(ars: AllocationRecoveryState, committed: Boolean): Future[Unit] = Future.successful(())
  
  def allocationRecoveryComplete(ars: AllocationRecoveryState, commit: Boolean): Future[Unit] = Future.successful(())
  
  def getObject(pointer: ObjectPointer): Future[Either[ObjectReadError, (ObjectMetadata, DataBuffer, List[Lock], Set[UUID])]] = Future.successful(Left(new InvalidLocalPointer))
  
  def getObjectMetadata(pointer: ObjectPointer): Future[Either[ObjectReadError, (ObjectMetadata, List[Lock], Set[UUID])]] = Future.successful(Left(new InvalidLocalPointer))
  
  def getObjectData(pointer: ObjectPointer): Future[Either[ObjectReadError, (DataBuffer, List[Lock], Set[UUID])]]= Future.successful(Left(new InvalidLocalPointer))
   
  def lockTransaction(txd: TransactionDescription, updateData: List[LocalUpdate],
                      preTransactionRebuilds: List[PreTransactionOpportunisticRebuild] = Nil): Future[List[ObjectTransactionError]] = Future.successful(Nil)

  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: List[LocalUpdate]): Future[List[UUID]] = Future.successful(Nil)
  
  def discardTransaction(txd: TransactionDescription): Unit = ()
  
  def pollAndRepairMissedUpdates(system: AspenSystem): Unit = ()
  
  def opportunisticRebuild(message: OpportunisticRebuild): Future[Unit] = Future.unit
}

object NullDataStore {
  val revision = ObjectRevision(new UUID(0,1))
  val refcount = ObjectRefcount(1,1)
}