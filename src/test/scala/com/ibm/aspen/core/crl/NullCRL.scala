package com.ibm.aspen.core.crl

import com.ibm.aspen.core.transaction.TransactionRecoveryState
import scala.concurrent.Future
import com.ibm.aspen.core.transaction.TransactionDescription
import java.nio.ByteBuffer

class NullCRL extends CrashRecoveryLog {
  def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = Future.successful(())
  
  def discardTransactionState(txd: TransactionDescription): Unit = ()
}