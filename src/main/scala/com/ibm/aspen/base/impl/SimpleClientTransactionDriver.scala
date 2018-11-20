package com.ibm.aspen.base.impl

import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.LocalUpdate

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.transaction.paxos.ProposalID
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxPrepareResponse
import org.apache.logging.log4j.scala.Logging

object SimpleClientTransactionDriver {
  
  def factory(retransmitDelay: Duration)(implicit ec: ExecutionContext): ClientTransactionDriver.Factory = {
    def f(
      messenger: ClientSideTransactionMessenger,
      txd: TransactionDescription, 
      updateData: Map[DataStoreID, List[LocalUpdate]]): ClientTransactionDriver = new SimpleClientTransactionDriver(retransmitDelay, messenger, txd, updateData)  
    
    f
  }
 
  
}

class SimpleClientTransactionDriver(
    val retransmitDelay: Duration,
    messenger: ClientSideTransactionMessenger,
    txd: TransactionDescription, 
    updateData: Map[DataStoreID, List[LocalUpdate]])
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
      
      val updateContent = updateData.get(toStore) match {
        case None => Nil
        case Some(lst) => if (haveUpdateContent.contains(toStore)) Nil else lst
      }
      
      messenger.send(initialPrepare, updateContent)
    }
  }
  
}