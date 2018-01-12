package com.ibm.aspen.core.data_store

import java.util.UUID
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.concurrent.Future
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
  
  /** Defines the Storage Pool this store belongs to and the Index of this store within the pool */
  def storeId: DataStoreID
  
  /** Shuts down the store and releases all runtime resources
   */
  def close(): Future[Unit]
  
  /** Used during the initialization process to determine which objects should start off in a locked state.
   * 
   */
  def getTransactionsToBeLocked(transactionRecoveryStates: List[TransactionRecoveryState]): List[TransactionRecoveryState] = {
    transactionRecoveryStates.filter(trs => trs.disposition == TransactionDisposition.VoteCommit)
  }
  
  /** Allocates new objects on the store.
   *
   * The "Allocating Object" is the object that will be updated as part of a successful allocation transaction.
   * Success/failure of this allocation is determined by the successful update of this object by a transaction
   * with the specified UUID. If success/failure cannot be determined, the recovery process may attempt to 
   * force the allocation to fail by bumping the revision of the target object.
   */
  def allocate(newObjects: List[Allocate.NewObject],
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
  def allocationRecoveryComplete(ars: AllocationRecoveryState, commit: Map[UUID, Boolean]): Future[Unit]
  
  /** Reads an object on the store 
   *
   *  This is the method that should be overridden by subclasses. The getObject method that accepts only the objectPointer checks to ensure that
   *  this store hosts the object before calling this method to do the actual fetch.  
   */
  protected def getObject(objectPointer: ObjectPointer, storePointer: StorePointer): Future[Either[ObjectReadError, (StoreObjectState,DataBuffer)]]
  
  
  /** Reads an object on the store */
  def getObject(objectPointer: ObjectPointer): Future[Either[ObjectReadError, (StoreObjectState,DataBuffer)]] = {
    objectPointer.storePointers.find(_.poolIndex == storeId.poolIndex) match {
      case Some(sp) => getObject(objectPointer, sp)
      case None => Future.successful(Left(new InvalidLocalPointer))
    }
  }
  
  
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
  
  protected class TransactionErrorChecker(txd: TransactionDescription, updateData: Option[List[LocalUpdate]]) {
    
    val localObjects = txd.allReferencedObjectsSet.foldLeft(List[(ObjectPointer, StorePointer)]())((l, op) => {
      if (op.poolUUID == storeId.poolUUID) {
        op.storePointers.find(_.poolIndex == storeId.poolIndex) match {
          case Some(sp) => (op, sp) :: l
          case None => l
        }
      } else
        l
    })
    
    var requiredRevisions = Map[UUID, ObjectRevision]()
    var requiredRefcounts = Map[UUID, ObjectRefcount]()
    var requiredData = Set[UUID]()
    
    txd.requirements.foreach { r => r match {
      case du: DataUpdate =>  
        requiredRevisions += (r.objectPointer.uuid -> du.requiredRevision)
        requiredData += r.objectPointer.uuid
        
      case vb: VersionBump =>  
        requiredRevisions += (r.objectPointer.uuid -> vb.requiredRevision)
        
      case ru: RefcountUpdate => 
        requiredRefcounts += (r.objectPointer.uuid -> ru.requiredRefcount)
    }}
    
    val updates = updateData match {
      case None => Set[UUID]()
      case Some(lst) => lst.foldLeft(Set[UUID]())((s, lu) => s + lu.objectUUID)
    }
    
    def getErrors( getCurrentState: (ObjectPointer, StorePointer) => Either[ObjectReadError, (ObjectRevision, ObjectRefcount, Option[TransactionDescription])] 
                 ): List[ObjectTransactionError] = {
      localObjects.foldLeft(List[ObjectTransactionError]()) { (l, t) =>
        val (op, sp) = t
        case class Err(e: ObjectTransactionError) extends Throwable
        try {
          getCurrentState(op, sp) match {
            case Left(err) => throw Err(TransactionReadError(op, err))
            
            case Right((currentRevision, currentRefcount, lockedTransaction)) =>
              
              if (!requiredRevisions.contains(op.uuid) && !requiredRefcounts.contains(op.uuid))
                throw Err(TransactionReadError(op, ObjectMismatch()))
              
              if (requiredData.contains(op.uuid) && !updates.contains(op.uuid)) throw Err(MissingUpdateContent(op))
              
              requiredRevisions.get(op.uuid).foreach(req => if (req != currentRevision) throw Err(RevisionMismatch(op, req, currentRevision)))
                
              requiredRefcounts.get(op.uuid).foreach(req => if (req != currentRefcount) throw Err(RefcountMismatch(op, req, currentRefcount)))
              
              lockedTransaction.foreach(ltxd => if (ltxd.transactionUUID != txd.transactionUUID) throw Err(TransactionCollision(op, ltxd)))
              
              l
          }
        } catch {
          case Err(e) => e :: l
        }
      }
    }
  }
}

