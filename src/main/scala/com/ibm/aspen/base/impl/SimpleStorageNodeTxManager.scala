package com.ibm.aspen.base.impl

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.transaction.TransactionDriver
import com.ibm.aspen.core.transaction.TransactionFinalizer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ Duration, MILLISECONDS }
import java.util.UUID

/** Provides simple mechanism for detecting and recovering from stalled transactions */
class SimpleStorageNodeTxManager(
    val heartbeatPeriod: Duration,
    val heartbeatTimeout: Duration,
    crl: CrashRecoveryLog,
    txresult: (UUID) => Option[Boolean],
    messenger: StoreSideTransactionMessenger,
    driverFactory: TransactionDriver.Factory,
    finalizerFactory: TransactionFinalizer.Factory)
    (implicit ec: ExecutionContext) extends StorageNodeTransactionManager(crl, txresult, messenger, driverFactory, finalizerFactory) {
  
  // Periodically send out heartbeats for all driven transactions and look for transactions that haven't received
  // heartbeats within the timeout window.
  BackgroundTask.schedulePeriodic(heartbeatPeriod) {
    val now = System.currentTimeMillis()
    
    getStores().valuesIterator.foreach { store =>
      val (transactions, drivers) = store.getTransactions()
      
      drivers.valuesIterator.foreach { driver => driver.heartbeat() }
      
      transactions.valuesIterator.foreach { t =>
        if ( Duration(now - t.lastHeartbeat, MILLISECONDS) > heartbeatTimeout && ! drivers.contains(t.txd.transactionUUID) )
          store.driveTransaction(t.txd)
      }
    }
  }
  
  
}