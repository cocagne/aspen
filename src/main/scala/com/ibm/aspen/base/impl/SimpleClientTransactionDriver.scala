package com.ibm.aspen.base.impl

import com.ibm.aspen.core.transaction._
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.data_store.DataStoreID

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import com.ibm.aspen.core.transaction.paxos.ProposalID
import org.apache.logging.log4j.scala.Logging

object SimpleClientTransactionDriver {
  
  def factory(retransmitDelay: Duration)(implicit ec: ExecutionContext): ClientTransactionDriver.Factory = {
    def f(
      messenger: ClientSideTransactionMessenger,
      txd: TransactionDescription, 
      updateData: Map[DataStoreID, (List[LocalUpdate], List[PreTransactionOpportunisticRebuild])]): ClientTransactionDriver = new SimpleClientTransactionDriver(retransmitDelay, messenger, txd, updateData)
    
    f
  }
 
  
}

class SimpleClientTransactionDriver(
    val retransmitDelay: Duration,
    messenger: ClientSideTransactionMessenger,
    txd: TransactionDescription, 
    updateData: Map[DataStoreID, (List[LocalUpdate], List[PreTransactionOpportunisticRebuild])])
    (implicit ec: ExecutionContext) extends ClientTransactionDriver(messenger, txd, updateData) with Logging {
  
  private var haveUpdateContent = Set[DataStoreID]()
  private var responded = Set[DataStoreID]()

  private var retries = 0

  private val task = BackgroundTask.schedulePeriodic(retransmitDelay) {
    synchronized {
      retries += 1
      if (retries % 3 == 0)
        logger.info(s"***** HUNG Client Transaction ${txd.transactionUUID}")
    }
    retransmit()
  }
  
  override def shutdown(): Unit = task.cancel()
  
  result onComplete { _ => task.cancel() }
  
  override def receive(prepareResponse: TxPrepareResponse): Unit = synchronized {
    haveUpdateContent += prepareResponse.from
  }
  
  override def receive(acceptResponse: TxAcceptResponse): Unit = synchronized {
    responded += acceptResponse.from
    super.receive(acceptResponse)
  }
  
  def retransmit(): Unit = synchronized {

    val poolUUID = txd.primaryObject.poolUUID
    val fromStore = DataStoreID(poolUUID, txd.designatedLeaderUID)
    val pid = ProposalID.initialProposal(txd.designatedLeaderUID)
    
    txd.allDataStores.filter(!responded.contains(_)).foreach { toStore =>
      val initialPrepare = TxPrepare(toStore, fromStore, txd, pid)
      
      val (updateContent, preTxRebuild) = updateData.get(toStore) match {
        case None => (Nil, Nil)
        case Some(tpl) => if (haveUpdateContent.contains(toStore)) (Nil, Nil) else tpl
      }
      
      messenger.send(initialPrepare, TransactionData(updateContent, preTxRebuild))
    }
  }
  
}