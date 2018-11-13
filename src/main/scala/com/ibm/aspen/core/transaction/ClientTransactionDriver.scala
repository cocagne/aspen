package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.transaction.paxos.{Learner, ProposalID}

import scala.concurrent.{Future, Promise}

object ClientTransactionDriver {
  type Factory = (ClientSideTransactionMessenger, TransactionDescription, Map[DataStoreID, List[LocalUpdate]]) => ClientTransactionDriver
 
  def noErrorRecoveryFactory(
    messenger: ClientSideTransactionMessenger,
    txd: TransactionDescription, 
    updateData: Map[DataStoreID, List[LocalUpdate]]): ClientTransactionDriver = new ClientTransactionDriver(messenger, txd, updateData)
}

class ClientTransactionDriver(
    val messenger: ClientSideTransactionMessenger,
    val txd: TransactionDescription, 
    val updateData: Map[DataStoreID, List[LocalUpdate]]) {
  
  protected val learner = new Learner(txd.primaryObject.ida.width, txd.primaryObject.ida.writeThreshold)
  protected val promise: Promise[Boolean] = Promise()
  
  def result: Future[Boolean] = promise.future
  
  def begin(): Unit = sendPrepareMessages()
  
  def shutdown(): Unit = {}

  private def complete(committed: Boolean): Unit = if (!promise.isCompleted) {
    HLCTimestamp.update(HLCTimestamp(txd.startTimestamp))
    promise.success(committed)
  }
  
  def receive(acceptResponse: TxAcceptResponse): Unit = synchronized {
    if (promise.isCompleted)
      return
      
    acceptResponse.response match {
      case Left(nack) => // Nothing to do
      case Right(accepted) => 
        learner.receiveAccepted(paxos.Accepted(acceptResponse.from.poolIndex, acceptResponse.proposalId, accepted.value)) match {
          case None => 
          case Some(committed) => complete(committed)
        }   
    }
  }
  
  def receive(prepareResponse: TxPrepareResponse): Unit = {}
  
  def receive(finalized: TxFinalized): Unit = synchronized { complete(finalized.committed) }

  def receive(resolved: TxResolved): Unit = synchronized { complete(resolved.committed) }
  
  protected def sendPrepareMessages(): Unit = {
    val poolUUID = txd.primaryObject.poolUUID
    
    val fromStore = DataStoreID(poolUUID, txd.designatedLeaderUID)
    
    txd.allDataStores.foreach { toStore =>
      val initialPrepare = TxPrepare(toStore, fromStore, txd, ProposalID.initialProposal(txd.designatedLeaderUID))
      
      val updateContent = updateData.get(toStore) match {
        case None => Nil
        case Some(lst) => lst
      }
      
      messenger.send(initialPrepare, updateContent)
    }
  }
}