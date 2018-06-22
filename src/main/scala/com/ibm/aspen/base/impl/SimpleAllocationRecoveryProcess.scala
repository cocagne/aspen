package com.ibm.aspen.base.impl

import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.allocation.AllocationStatusReply
import com.ibm.aspen.core.network.StoreSideAllocationMessenger
import com.ibm.aspen.core.data_store.DataStore
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.allocation.AllocationObjectStatus
import scala.concurrent.duration.Duration
import com.ibm.aspen.core.allocation.AllocationStatusRequest
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.concurrent.Future
import com.ibm.aspen.base.ExponentialBackoffRetryStrategy
import com.ibm.aspen.base.ExponentialBackoffRetryStrategy
import java.util.UUID
import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.util.uuid2byte
import com.ibm.aspen.base.tieredlist.TieredKeyValueList

/** Implements the Allocation Recovery Process.
 * 
 * Error retry mechanism is left to subclasses to implement.
 * 
 * General approach is to use a background task that periodically queries status of the allocating object.
 * Its purpose is to determine whether the revision of the object has changed and, if not, to force it to
 * change.
 * 
 * Once the revision is known to have changed AND the transaction status in a threshold number of replies
 * is seen to be "Unknown" (which means the transaction has definitely completed), we attempt to look up
 * each object in the allocation tree. Once we have an exists/not-exists, we give this information over
 * to the data store for commit and remove from the CRL when done.
 * 
 *     Note: It is possible for an allocation transaction to succeed but for some or all of the objects
 *           to have been deleted before this recovery process completes. Example, Alloc 5 objects, crash,
 *           delete 2 objects, recover. In that case 3 objects should be committed and 2 discarded.
 */
class SimpleAllocationRecoveryProcess(
    val statusQueryPeriod: Duration,
    val system: BasicAspenSystem,
    val allocMessenger: StoreSideAllocationMessenger,
    val crl: CrashRecoveryLog,
    val store: DataStore,
    val ars: AllocationRecoveryState)(implicit ec: ExecutionContext) {

  private[this] var replies = Map[DataStoreID, AllocationStatusReply]()
  private[this] var revisionChanged = false
  private[this] var resolving = false
  private[this] var bumpingVersion = false
  private[this] val promise = Promise[Boolean]()
  private[this] val retryStrategy = new ExponentialBackoffRetryStrategy
  
  def done: Future[Boolean] = promise.future
  
  private[this] val queryTask = BackgroundTask.schedulePeriodic(statusQueryPeriod) { synchronized {
    replies = Map()
    
    ars.allocatingObject.storePointers.foreach { sp =>
      val to = DataStoreID(ars.allocatingObject.poolUUID, sp.poolIndex)
      allocMessenger.send(AllocationStatusRequest(to, store.storeId, ars.allocatingObject, ars.allocationTransactionUUID, ars.newObjectUUID))
    }
  }}
  
  def shutdown(): Unit = {
    queryTask.cancel()
    retryStrategy.shutdown()
  }
  
  def receive(m: AllocationStatusReply): Unit = synchronized {
    replies += (m.from -> m)
    
    val currentRevision = if (replies.values.count(m => m.transactionStatus.isEmpty) >= ars.allocatingObject.ida.restoreThreshold) {
      val errUUID = new UUID(0,0) // Object could be deleted (quorum of errors)
      
      var revisionCounts = replies.values.foldLeft(Map[UUID, Int]()) { 
        (m, as) => as.objectStatus.state match {
          case Left(err) => m.get(errUUID) match {
            case Some(count) => m + (errUUID -> (count+1))
            case None => m + (errUUID -> 1)
          }
          case Right(status) => m.get(status.revision.lastUpdateTxUUID) match {
            case Some(count) => m + (status.revision.lastUpdateTxUUID -> (count+1))
            case None => m + (status.revision.lastUpdateTxUUID -> 1)
          }
        }
      }
      
      revisionCounts.find( t => t._2 >= ars.allocatingObject.ida.restoreThreshold ).map( t => t._1 )
    } 
    else
      None
      
    currentRevision foreach { current =>
      if ( current == ars.allocatingObjectRevision.lastUpdateTxUUID )
        bumpAllocatingObjectVersion()
      else if (!resolving) {
        resolving = true
        resolveAllocation()
      }
    }
  }
  
  private[this] def bumpAllocatingObjectVersion() = synchronized {
    if (!bumpingVersion) {
      bumpingVersion = true
      val tx = system.newTransaction()
      
      tx.bumpVersion(ars.allocatingObject, ars.allocatingObjectRevision)
      
      // It's okay if this fails. The retry mechanism ensures it is repeatedly called while the version remains
      // the same. The result is ignored for the sake of simplicity. We'll just rely on the retry mechanism to
      // eventually figure out that the version has changed or re-try the version bump.
      tx.commit() onComplete { _ =>
        synchronized {
          bumpingVersion = false
        }
      }
    }
  }
  
  /** called after the revision is known to have changed and the allocation transaction is finalized. */
  private[this] def resolveAllocation(): Unit = {
    
    queryTask.cancel()
    
    def resolveObject(objectUUID: UUID, allocTree: TieredKeyValueList): Future[(UUID, Boolean)] = for {
      entry <- allocTree.get( objectUUID )
    } yield {
      (objectUUID, entry.isDefined)
    }
    
    retryStrategy.retryUntilSuccessful {
      for {
        pool <- system.getStoragePool(store.storeId.poolUUID)
        allocTree <- pool.getAllocationTree(retryStrategy)
        (objId, committed) <- resolveObject(ars.newObjectUUID, allocTree)
        complete <- store.allocationRecoveryComplete(ars, committed)
      } yield {
        promise.success(committed)
      }
    } 
  }
}