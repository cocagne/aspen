package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.Proposer
import com.ibm.aspen.core.transaction.paxos.Learner
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.ExecutionContext

abstract class TransactionDriver(
    val storeId: DataStoreID,
    val messenger: StoreSideTransactionMessenger, 
    initialPrepare: TxPrepare, 
    private val finalizerFactory: TransactionFinalizer.Factory,
    private val onComplete: (UUID) => Unit)(implicit ec: ExecutionContext) {
  
  import TransactionDriver._
  
  val txd = initialPrepare.txd
  def ida = txd.primaryObject.ida
  
  protected val proposer = new Proposer(storeId.poolIndex, ida.width, ida.writeThreshold)
  protected val learner = new Learner(ida.width, ida.writeThreshold)
  protected val validAcceptorSet = txd.primaryObject.storePointers.iterator.map(sp => sp.poolIndex).toSet
  protected val allObjects = txd.allReferencedObjectsSet
  protected val primaryObjectDataStores = txd.primaryObjectDataStores
  protected val allDataStores = txd.allDataStores.toList
  
  protected var finalized = false
  protected var peerDispositions = Map[DataStoreID, TransactionDisposition.Value]()
  protected var acceptedPeers = Set[DataStoreID]()
  protected var finalizer: Option[TransactionFinalizer] = None
 
  protected def isValidAcceptor(ds: DataStoreID) = ds.poolUUID == txd.primaryObject.poolUUID && validAcceptorSet.contains(ds.poolIndex)
  
  def receiveTxPrepare(msg: TxPrepare): Unit = synchronized {
      proposer.updateHighestProposalId(msg.proposalId)
  }
  
  
  // TODO: This implementation takes action immediately upon receiving replies from a write-threshold number
  //       of peers. A better approach would probably be to wait a short while for additional replies before
  //       making a decision on whether to commit/abort.
  //
  // TODO: Look for collisions with other transactions and abort only if their timestamp is less than ours
  //
  def receiveTxPrepareResponse(msg: TxPrepareResponse): Unit = synchronized {
    
    if (msg.proposalId != proposer.currentProposalId)
      return
      
    msg.response match {
      case Left(nack) => 
        onNack(nack.promisedId)
      
      case Right(promise) => 
        peerDispositions += (msg.from -> msg.disposition)
        
        if (isValidAcceptor(msg.from))
          proposer.receivePromise(paxos.Promise(msg.from.poolIndex, msg.proposalId, promise.lastAccepted))
            
        if (proposer.prepareQuorumReached && !proposer.haveLocalProposal) {
          
          var canCommitTransaction = true
          
          // Before we can make a decision, we must ensure that we have a write-threshold number of replies for
          // each of the objects referenced by the transaction
          for (op <- allObjects) {
            var nReplies = 0
            var canCommitObject = true
            
            op.storePointers.foreach(sp => peerDispositions.get(DataStoreID(op.poolUUID, sp.poolIndex)).foreach(disposition => {
              nReplies += 1
              disposition match {
                case TransactionDisposition.VoteCommit => 
                case _ =>
                  // TODO: We can be quite a bit smarter about choosing when we must abort
                  canCommitObject = false
              }
            }))
            
            if (nReplies < op.ida.writeThreshold)
              return
            
            // Once canCommitTransaction flips to false, it stays there
            canCommitTransaction = canCommitTransaction && canCommitObject
          }
          
          // If we get here, we've made our decision
          
          proposer.setLocalProposal(canCommitTransaction)
          
          sendAcceptMessages()
        }
    }
  }
  
  def receiveTxAcceptResponse(msg: TxAcceptResponse): Unit = synchronized {
    
    if (msg.proposalId != proposer.currentProposalId)
      return
      
    // We shouldn't ever receive an AcceptReponse from a non-acceptor but just to be safe...
    if (!isValidAcceptor(msg.from))
      return 
      
    msg.response match {
      case Left(nack) => 
        onNack(nack.promisedId)
      
      case Right(accepted) => 
        acceptedPeers += msg.from
        
        val alreadyResolved = learner.finalValue.isDefined
        
        learner.receiveAccepted(paxos.Accepted(msg.from.poolIndex, msg.proposalId, accepted.value)) match {
          case None => 
          case Some(committed) => if (!alreadyResolved) {
            // TODO: Wait a bit for additional responses before finalizing. This approach always shows only write-threshold
            //       peers successfully processed the transaction
            if (committed) {
              val f = finalizerFactory.create(txd, acceptedPeers, messenger)
              f.complete foreach {
                _ => onFinalized(committed) 
              }
              finalizer = Some(f)
            } else 
              onFinalized(false)
            
            onResolution(committed)
          }
        }   
    }
  }
  
  def receiveTxResolved(msg: TxResolved): Unit = {}
  
  def receiveTxFinalized(msg: TxFinalized): Unit = synchronized { 
    finalizer.foreach( _.cancel() )
    onFinalized(msg.committed)
  }
  
  def mayBeDiscarded: Boolean = synchronized { finalized }
  
  protected def onFinalized(committed: Boolean) = synchronized {
    if (!finalized) {
      finalized = true
      onComplete(txd.transactionUUID)
      
      txd.originatingClient.foreach(client => {
        messenger.send(client, TxFinalized(NullDataStoreId, storeId, txd.transactionUUID, committed))
      })
      
      val messages = allDataStores.map(toStoreId => TxFinalized(toStoreId, storeId, txd.transactionUUID, committed))
      messenger.send(messages)
    }
  }
  
  protected def nextRound(): Unit = {
    peerDispositions = Map()
    acceptedPeers = Set()
    proposer.nextRound()
  }
  
  protected def sendAcceptMessages(): Unit = {
    val poolUUID = txd.primaryObject.poolUUID
    
    val accept = synchronized {
      proposer.currentAcceptMessage().map(paxAccept => (paxAccept, acceptedPeers))
    }
    
    accept.foreach(t => {
      val (paxAccept, acceptedPeers) = t
      val messages = primaryObjectDataStores.filter(!acceptedPeers.contains(_)).map(toStoreId => 
        TxAccept(toStoreId, storeId, txd.transactionUUID, paxAccept.proposalId, paxAccept.proposalValue)
      ).toList
      messenger.send(messages)
    })
  }
  
  protected def onNack(promisedId: paxos.ProposalID): Unit = {
    proposer.updateHighestProposalId(promisedId)
    nextRound()
  }
  
  protected def onResolution(committed: Boolean): Unit = {
    val messages = (allDataStores.iterator ++ txd.notifyOnResolution.iterator).map(toStoreId => TxResolved(toStoreId, storeId, txd.transactionUUID, committed)).toList
    messenger.send(messages)
    txd.originatingClient.foreach(clientId => messenger.send(clientId, TxResolved(NullDataStoreId, storeId, txd.transactionUUID, committed)))
  }
}

object TransactionDriver {
  /** Used when sending Transaction Messages to Clients instead of data stores */
  val NullDataStoreId = DataStoreID(new UUID(0,0), 0)
  
  trait Factory {
    def create(
        storeId: DataStoreID,
        messenger:StoreSideTransactionMessenger, 
        initialPrepare: TxPrepare, 
        finalizerFactory: TransactionFinalizer.Factory,
        onComplete: (UUID) => Unit): TransactionDriver
  }
}
