package com.ibm.aspen.core.data_store

import java.util.UUID
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.allocation.AllocationErrors
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.transaction.TransactionDisposition
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.transaction.DataUpdate
import com.ibm.aspen.core.transaction.RefcountUpdate
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.transaction.VersionBump
import com.ibm.aspen.core.allocation.AllocationOptions
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.read.OpportunisticRebuild

object DataStore {
  trait Factory {
    
    /** The future completes with a reference to itself when the store is ready for use.
     *
     * A critical requirement is that the store re-establishes all object locks for outstanding transactions it voted to commit
     */
    def apply(
        storeId: DataStoreID,
        transactionRecoveryStates: List[TransactionRecoveryState],
        allocationRecoveryStates: List[AllocationRecoveryState]): Future[DataStore]
  }
}

trait DataStore {
  
  implicit val executionContext: ExecutionContext
  
  /** Defines the Storage Pool this store belongs to and the Index of this store within the pool */
  def storeId: DataStoreID
  
  /** Completes when the store is fully initialized and ready for use */
  val initialized: Future[DataStore]
  
  /** Shuts down the store and releases all runtime resources */
  def close(): Future[Unit]
  
  def maximumAllowedObjectSize: Option[Int] = None
  
  /** Allocates a new Object on the store during the system bootstrapping process */
  def bootstrapAllocateNewObject(objectUUID: UUID, initialContent: DataBuffer, timestamp: HLCTimestamp): Future[StorePointer]
  
  /** Overwrites the object content during the system bootstrapping process. Future is to data at rest on disk */
  def bootstrapOverwriteObject(objectPointer: ObjectPointer, newContent: DataBuffer, timestamp: HLCTimestamp): Future[Unit]
  
  /** Allocates new objects on the store.
   *
   * The "Allocating Object" is the object that will be updated as part of a successful allocation transaction.
   * Success/failure of this allocation is determined by the successful update of this object by a transaction
   * with the specified UUID. If success/failure cannot be determined, the recovery process may attempt to 
   * force the allocation to fail by bumping the revision of the target object.
   */
  def allocate(newObjectUUID: UUID,
               options: AllocationOptions,
               objectSize: Option[Int],
               initialRefcount: ObjectRefcount,
               objectData: DataBuffer,
               timestamp: HLCTimestamp,
               allocationTransactionUUID: UUID,
               allocatingObject: ObjectPointer,
               allocatingObjectRevision: ObjectRevision): Future[Either[AllocationErrors.Value, AllocationRecoveryState]]
  
  /** Called by the AllocationManager when the commit transaction is resolved. The AllocationRecoveryState will
   *  be deleted after the returned future completes
   */
  def allocationResolved(ars: AllocationRecoveryState, committed: Boolean): Future[Unit]
  
  
  /** Called by the allocation recovery process when the state of each pending object allocation is determined.
   *  The supplied commit map indicates the commit/abort state for each of the objects.
   *  
   *  The AllocationRecoveryState will be deleted after the returned future completes
   */
  def allocationRecoveryComplete(ars: AllocationRecoveryState, commit: Boolean): Future[Unit]
  
  
  /** Reads and returns the object metadata, data, and a list of any active locks on the object */
  def getObject(pointer: ObjectPointer): Future[Either[ObjectReadError, (ObjectMetadata, DataBuffer, List[Lock])]]
  
  
  /** Returns the object metadata but not the object data itself.
   *  This may be used to optimize reads on DataStores that separate object metadata from the data itself. Whenever read
   *  and transaction requests can be satisfied without reading the object data, this method will be used instead of
   *  getObject
   */
  def getObjectMetadata(pointer: ObjectPointer): Future[Either[ObjectReadError, (ObjectMetadata, List[Lock])]]
  
  
  /** Returns the object data without the metadata.
   *  This may be used to optimize reads on DataStores that separate object metadata from the data itself. Whenever
   *  read and transaction requests can be satisfied without reading the object metadata, this method will be used
   *  instead of getObject
   */
  def getObjectData(pointer: ObjectPointer): Future[Either[ObjectReadError, (DataBuffer, List[Lock])]]
  
  
  /** Attempts to locks all objects referenced by the transaction that are hosted by this store.
   *  
   *  If the returned list of errors is empty, the transaction successfully locked all objects. If any errors are returned,
   *  no object locks are granted.
   */
  def lockTransaction(txd: TransactionDescription, updateData: Option[List[LocalUpdate]]): Future[List[ObjectTransactionError]]
  
  
  /** Commits the transaction changes and returns a Future to the completion of the commit operation.
   *  
   *  This method always returns Success() since there are no recovery steps the transaction logic can take for failures
   *  that occur after the commit decision has been made. 
   */
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[List[LocalUpdate]]): Future[Unit]
  
  
  /** Called at the end of each transaction to ensure all object locks are released.
   *  
   *  For successful transactions, commitTransactionUpdates will be called first and it should release the
   *  locks while the finalization actions run. Both committed and aborted transactions call this method.
   * 
   */
  def discardTransaction(txd: TransactionDescription): Unit
  
  
  /** Called to check for and repair objects that missed updates */
  def pollAndRepairMissedUpdates(system: AspenSystem): Unit
  
  /** Called when an OpportunisticRebuild message is sent by a reader that noticed this store has
   *  missed an update to a hosted object
   */
  def opportunisticRebuild(message: OpportunisticRebuild): Unit
}

