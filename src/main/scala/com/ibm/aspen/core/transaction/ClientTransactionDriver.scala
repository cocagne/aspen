package com.ibm.aspen.core.transaction

import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.paxos.Learner
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.transaction.paxos.ProposalID
import com.ibm.aspen.core.data_store.DataStoreID
import scala.concurrent.Promise
import scala.concurrent.Future

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
  protected val promise = Promise[Boolean]()
  
  def result: Future[Boolean] = promise.future
  
  // Send initial set of messages
  sendPrepareMessages()
  
  def receive(acceptResponse: TxAcceptResponse): Unit = synchronized {
    if (promise.isCompleted)
      return
      
    acceptResponse.response match {
      case Left(nack) => // Nothing to do
      case Right(accepted) => 
        learner.receiveAccepted(paxos.Accepted(acceptResponse.from.poolIndex, acceptResponse.proposalId, accepted.value)) match {
          case None => 
          case Some(committed) => 
            if (!promise.isCompleted)
              promise.success(committed)
        }   
    }
  }
  
  def receive(finalized: TxFinalized): Unit = synchronized {
    if (!promise.isCompleted)
      promise.success(finalized.committed)
  }
  
  protected def sendPrepareMessages(): Unit = {
    val poolUUID = txd.primaryObject.poolUUID
    
    val heardFrom = learner.peerBitset
    
    txd.primaryObject.storePointers.foreach(sp => {
      val to = DataStoreID(poolUUID, sp.poolIndex)
      val from = DataStoreID(poolUUID, txd.designatedLeaderUID)
      val initialPrepare = TxPrepare(to, from, txd, ProposalID(0, txd.designatedLeaderUID))
      if (!heardFrom.get(sp.poolIndex)) {
        val updateContent = updateData.get(to) match {
          case None => Nil
          case Some(lst) => lst
        }
        messenger.send(initialPrepare, updateContent)
      }
    })
  }
}