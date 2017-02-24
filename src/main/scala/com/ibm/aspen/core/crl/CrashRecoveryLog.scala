package com.ibm.aspen.core.crl

import com.ibm.aspen.core.transaction.TransactionRecoveryState
import scala.concurrent.Future

trait CrashRecoveryLog {
  
  /** Returns a Future to successfully saving the transaction state.
   *
   * Note: Failure will be returned if the recovery state cannot be saved. This can happen if
   *       if the media hosting the state fails  
   */
  def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit]
}