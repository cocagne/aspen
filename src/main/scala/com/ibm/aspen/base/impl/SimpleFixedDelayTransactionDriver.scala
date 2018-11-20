package com.ibm.aspen.base.impl

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.TransactionFinalizer
import java.util.UUID

import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.transaction.TransactionDriver
import com.ibm.aspen.core.transaction.TxResolved
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.duration.Duration

object SimpleFixedDelayTransactionDriver {
  
  def factory(retryDelay: Duration): TransactionDriver.Factory = {
    return new TransactionDriver.Factory {
      def create(
          storeId: DataStoreID,
          messenger:StoreSideTransactionMessenger, 
          txd: TransactionDescription, 
          finalizerFactory: TransactionFinalizer.Factory)(implicit ec: ExecutionContext): TransactionDriver = {
        new SimpleFixedDelayTransactionDriver(retryDelay, storeId, messenger, txd, finalizerFactory)
      }
    }
  }
}

class SimpleFixedDelayTransactionDriver(
    retryDelay: Duration,
    storeId: DataStoreID,
    messenger: StoreSideTransactionMessenger, 
    txd: TransactionDescription, 
    finalizerFactory: TransactionFinalizer.Factory)(implicit ec: ExecutionContext) extends TransactionDriver(
  storeId, messenger, txd, finalizerFactory) with Logging {

  private var retries = 0

  private[this] var scheduledRetry = BackgroundTask.schedulePeriodic(retryDelay) {
    synchronized {
      retries += 1
      if (retries % 3 == 0) {
        logger.info(s"***** HUNG Transaction ${txd.transactionUUID}")
        printState(s => logger.info(s"* $s"))
      }
    }
    nextRound()
    sendPrepareMessages()
  }
  
  override def receiveTxResolved(msg: TxResolved): Unit = {
    super.receiveTxResolved(msg)
    scheduledRetry.cancel()
  }
  
  override protected def onFinalized(committed: Boolean): Unit = {
    super.onFinalized(committed)
    scheduledRetry.cancel()
  }
}