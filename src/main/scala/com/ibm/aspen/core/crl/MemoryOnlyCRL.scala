package com.ibm.aspen.core.crl

import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.transaction.LocalUpdateContent
import scala.concurrent.Future
import scala.concurrent.Promise

object MemoryOnlyCRL extends CrashRecoveryLog {
  
  private val queue = new java.util.concurrent.LinkedBlockingQueue[Promise[Unit]]()
  
  override def saveTransactionRecoveryState(state: TransactionRecoveryState, dataUpdateContent: Option[LocalUpdateContent]): Future[Unit] = {
    val p = Promise[Unit]()
    queue.put(p)
    p.future
  }
  
  override def discardTransactionState(txd: TransactionDescription): Unit = {}
  
  private val backgroundThread = new Thread("MemoryOnlyCRL") {
    override def run(): Unit = while(true) queue.take().success(())
  }
  
  backgroundThread.setDaemon(true)
  backgroundThread.start()
}