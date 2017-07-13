package com.ibm.aspen.core.transaction

import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.paxos.Learner
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.transaction.paxos.ProposalID
import com.ibm.aspen.core.data_store.DataStoreID
import scala.concurrent.Promise

class ClientTransactionDriver(
    val messenger: ClientSideTransactionMessenger,
    val txd: TransactionDescription, 
    val updateData: List[Map[Byte,ByteBuffer]]) {
  
  protected val learner = new Learner(txd.primaryObject.ida.width, txd.primaryObject.ida.writeThreshold)
  protected val promise = Promise[Boolean]()
  
  // Send initial set of messages
  sendPrepareMessages()
  
  def receive(fromStore: DataStoreID, acceptResponse: TxAcceptResponse): Unit = synchronized {
    if (promise.isCompleted)
      return
      
    acceptResponse.response match {
      case Left(nack) => // Nothing to do
      case Right(accepted) => 
        learner.receiveAccepted(paxos.Accepted(acceptResponse.from.poolIndex, acceptResponse.proposalId, accepted.value)) match {
          case None => 
          case Some(committed) => promise.success(committed)
        }   
    }
  }
  
  def receive(fromStore: DataStoreID, finalized: TxFinalized): Unit = synchronized {
    if (!promise.isCompleted)
      promise.success(finalized.committed)
  }
  
  protected def sendPrepareMessages(): Unit = {
    val poolUUID = txd.primaryObject.poolUUID
    
    val initialPrepare = TxPrepare(DataStoreID(poolUUID, txd.designatedLeaderUID), 
                                   txd, ProposalID(0, txd.designatedLeaderUID))
    
    val heardFrom = learner.peerBitset
    
    txd.primaryObject.storePointers.foreach(sp => {
      val dest = DataStoreID(poolUUID, sp.poolIndex)
      if (!heardFrom.get(sp.poolIndex)) {
        val updateContent = updateData.map( m => m(sp.poolIndex) )
        messenger.send(dest, initialPrepare, updateContent)
      }
    })
  }
}