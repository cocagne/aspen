package com.ibm.aspen.base.impl

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.TransactionFinalizer
import java.util.UUID
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.transaction.TransactionDriver
import com.ibm.aspen.core.transaction.TxResolved
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
  storeId, messenger, txd, finalizerFactory) {
  
  private[this] var scheduledRetry = BackgroundTask.schedulePeriodic(retryDelay, callNow=false) {
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