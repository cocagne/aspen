package com.ibm.aspen.core.data_store

import java.util.UUID
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.allocation.AllocationErrors
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.transaction.TransactionDisposition

trait DataStore {
  
  /** Defines the Storage Pool this store belongs to and the Index of this store within the pool */
  def storeId: DataStoreID
  
  /** Called by the StorageNode to initialize the data store. The future completes when the store is ready for use. No methods should be invoked prior to completion 
   *
   * A critical requirement of this method is that the store re-establishes locks on the transactions it voted to commit
   */
  def initialize(transactionRecoveryStates: List[TransactionRecoveryState]): Future[Unit]
  
  /** Shuts down the store and releases all runtime resources
   */
  def close(): Future[Unit]
  
  def getTransactionsToBeLocked(transactionRecoveryStates: List[TransactionRecoveryState]): List[TransactionRecoveryState] = {
    transactionRecoveryStates.filter(trs => trs.disposition == TransactionDisposition.VoteCommit)
  }
  
  /** Allocates a new Object on the store.
   *
   * The "Allocating Object" is the object that will be updated as part of a successful allocation transaction.
   * Success/failure of this allocation is determined by the successful update of this object by a transaction
   * with the specified UUID. If success/failure cannot be determined, this node may attempt to force the allocation
   * to fail by bumping the revision of the target object.
   */
  def allocateNewObject(objectUUID: UUID, 
                        size: Option[Int], 
                        initialContent: ByteBuffer,
                        initialRefcount: ObjectRefcount,
                        allocationTransactionUUID: UUID,
                        allocatingObject: ObjectPointer,
                        allocatingObjectRevision: ObjectRevision): Future[Either[AllocationErrors.Value, StorePointer]]
  
  
  /** Reads an object on the store */
  protected def getObject(objectPointer: ObjectPointer, storePointer: StorePointer): Future[Either[ObjectError.Value, (CurrentObjectState,ByteBuffer)]]
  
  
  /** Reads an object on the store */
  def getObject(objectPointer: ObjectPointer): Future[Either[ObjectError.Value, (CurrentObjectState,ByteBuffer)]] = {
    objectPointer.storePointers.find(_.poolIndex == storeId.poolIndex) match {
      case Some(sp) => getObject(objectPointer, sp)
      case None => Future.successful(Left(ObjectError.InvalidLocalPointer))
    }
  }
  
  
  /** Returns a future to a map of the current object state for all hosted objects referenced by the TransactionDescription
   *  
   *  This method always returns a Success(). Any errors encountered along the way are noted within the CurrentObjectState
   *  associated with the object(s) for which errors were encountered. 
   */
  def getCurrentObjectState(txd: TransactionDescription): Future[Map[UUID, Either[ObjectError.Value, CurrentObjectState]]] 
  
  
  /** Locks all objects referenced by the transaction or returns a map of collisions and/or errors. Note that this method
   *  must detect Revision and Refcount mismatch errors. getCurrentObjectState is used by transactions to do initial error
   *  checking on the refcount and revision but it is possible for those values to change between that call and this call.
   */
  def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, Either[ObjectError.Value, TransactionDescription]]]
  
  
  /** Commits the transaction changes and returns a Future to the completion of the commit operation.
   *  
   *  This method always returns Success() since there are no recovery steps the transaction logic can take for failures
   *  that occur after the commit decision has been made. 
   */
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[Array[ByteBuffer]]): Future[Unit]
  
  
  /** Called at the end of each transaction to ensure all object locks are released.
   *  
   *  For successful transactions, commitTransactionUpdates will be called first and it should release the
   *  locks while the finalization actions run. Both committed and aborted transactions call this method.
   * 
   */
  def discardTransaction(txd: TransactionDescription): Unit
}