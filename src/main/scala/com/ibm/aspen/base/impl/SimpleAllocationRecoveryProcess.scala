package com.ibm.aspen.base.impl

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}

/** Implements an extremely simple Allocation Recovery Process.
  *
  * 1. Use the revisionGuard to ensure that the potentially outstanding allocation transaction cannot succeed.
  * 2. Ensure all transaction finalization actions are complete
  * 3. Look up the object-being-allocated in the pool's allocation tree
  *
  * If the object is present in the allocation tree, commit the allocated object. Otherwise discard it.
  *
 */
class SimpleAllocationRecoveryProcess(
                                     system: AspenSystem,
                                     ars: AllocationRecoveryState
                                     )(implicit ec: ExecutionContext) extends Logging {

  private[this] val promise = Promise[Boolean]()

  def done: Future[Boolean] = promise.future

  ars.revisionGuard.ensureTransactionCannotCommit(system).onComplete { _ =>

    system.getTransactionFinalized(ars.revisionGuard.pointer, ars.allocationTransactionUUID).onComplete { _ =>

      system.retryStrategy.retryUntilSuccessful {
        for {
          pool <- system.getStoragePool(ars.storeId.poolUUID)
          allocTree <- pool.getAllocationTree(system.retryStrategy)
          entry <- allocTree.get(ars.newObjectUUID)
        } yield {
          promise.success(entry.isDefined)
        }
      }

    }

  }

}