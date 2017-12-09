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


class SimpleClientTransactionDriver(
    val retransmitDelay: Duration,
    messenger: ClientSideTransactionMessenger,
    txd: TransactionDescription, 
    updateData: Map[DataStoreID, List[LocalUpdate]])
    (implicit ec: ExecutionContext) extends ClientTransactionDriver(messenger, txd, updateData) {
  
  private var responded = Set[DataStoreID]()
  
  private val task = BackgroundTask.schedulePeriodic(retransmitDelay) {
    retransmit()
  }
  
  result onComplete { _ => task.cancel() }
  
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
        case Some(lst) => lst
      }
      
      messenger.send(initialPrepare, updateContent)
    }
  }
  
}