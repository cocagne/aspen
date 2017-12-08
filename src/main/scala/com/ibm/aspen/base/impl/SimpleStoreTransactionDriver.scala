package com.ibm.aspen.base.impl

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.transaction.TransactionFinalizer
import java.util.UUID
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.transaction.TransactionDriver
import scala.concurrent.duration.Duration

/** Provides a store-side transaction driver with a very simple retransmit strategy and exponential backoff mechanism
 *  for dealing with Paxos contention.
 */
class SimpleStoreTransactionDriver(
    val initialDelay: Duration, 
    val maxDelay: Duration,
    storeId: DataStoreID,
    messenger: StoreSideTransactionMessenger, 
    initialPrepare: TxPrepare, 
    finalizerFactory: TransactionFinalizer.Factory,
    onComplete: (UUID) => Unit)(implicit ec: ExecutionContext) extends TransactionDriver(storeId, messenger, initialPrepare, finalizerFactory, onComplete) {
 
  private[this] var backoffDelay = initialDelay
  private[this] var nextTry = BackgroundTask.schedule(initialDelay) { sendMessages() }
  
  private def sendMessages(): Unit = synchronized {
    proposer.currentAcceptMessage() match {
      case Some(_) => sendAcceptMessages()
      case None => 
        messenger.send(txd.allDataStores.map(toStore => TxPrepare(toStore, storeId, txd, proposer.currentProposalId)).toList)
    }
    
    // Continually re-broadcast the prepare/accept messages for our current proposal at a fixed rate
    // if we get interrupted, the backoff mechanism will protect against contention
    nextTry.cancel()
    nextTry = BackgroundTask.schedule(initialDelay) { sendMessages() }
  }
  
  override protected def nextRound(): Unit = synchronized {
    super.nextRound()
    
    backoffDelay = backoffDelay * 2
    
    if (backoffDelay > maxDelay)
      backoffDelay = maxDelay
      
    nextTry.cancel()
    nextTry = BackgroundTask.schedule(backoffDelay) { sendMessages() }
  }
  
}