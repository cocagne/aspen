package com.ibm.aspen.base.impl

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.transaction.{TransactionDriver, TransactionFinalizer}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, MILLISECONDS}

/** Provides simple mechanism for detecting and recovering from stalled transactions */
class SimpleStorageNodeTxManager(
    val heartbeatPeriod: Duration,
    val heartbeatTimeout: Duration,
    crl: CrashRecoveryLog,
    transactionCache: TransactionStatusCache,
    messenger: StoreSideTransactionMessenger,
    driverFactory: TransactionDriver.Factory,
    finalizerFactory: TransactionFinalizer.Factory)
    (implicit ec: ExecutionContext)
  extends StorageNodeTransactionManager(crl, transactionCache, messenger, driverFactory, finalizerFactory) with Logging {
  
  // Periodically send out heartbeats for all driven transactions and look for transactions that haven't received
  // heartbeats within the timeout window.
  BackgroundTask.schedulePeriodic(heartbeatPeriod) {
    val now = System.currentTimeMillis()
    
    storesSnapshot.valuesIterator.foreach { store =>
      val (transactions, drivers) = store.getTransactions
      
      drivers.valuesIterator.foreach { driver => driver.heartbeat() }
      
      transactions.valuesIterator.foreach { t =>
        if ( Duration(now - t.lastHeartbeat, MILLISECONDS) > heartbeatTimeout && ! drivers.contains(t.txd.transactionUUID) ) {
          logger.warn(s"TransactionDriver for ${t.txd.transactionUUID} timed out with delay ${Duration(now - t.lastHeartbeat, MILLISECONDS)} > $heartbeatTimeout")
          store.driveTransaction(t.txd)
        }
      }
    }
  }
  
  
}