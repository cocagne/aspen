package com.ibm.aspen.core.crl

import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.transaction.LocalUpdateContent
import scala.concurrent.Future
import com.ibm.aspen.core.transaction.TransactionDescription

class NullCRL extends CrashRecoveryLog {
  def saveTransactionRecoveryState(state: TransactionRecoveryState, dataUpdateContent: Option[LocalUpdateContent]): Future[Unit] = Future.successful(())
  
  def discardTransactionState(txd: TransactionDescription): Unit = ()
}