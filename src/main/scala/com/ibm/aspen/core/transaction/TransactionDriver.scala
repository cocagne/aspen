package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.network.TransactionMessenger
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.Proposer
import com.ibm.aspen.core.transaction.paxos.Learner
import com.ibm.aspen.core.objects.ObjectPointer

abstract class TransactionDriver(
    val storeId: DataStoreID,
    val messenger:TransactionMessenger, 
    initialPrepare: TxPrepare, 
    private val finalizerFactory: TransactionFinalizer.Factory,
    private val onComplete: (UUID) => Unit) {
  
  val ida = initialPrepare.txd.primaryObject.ida
  
  protected val proposer = new Proposer(storeId.poolIndex, ida.width, ida.writeThreshold)
  protected val learner = new Learner(ida.width, ida.writeThreshold)
  protected val validAcceptorSet = initialPrepare.txd.primaryObject.storePointers.iterator.map(sp => sp.poolIndex).toSet
  protected val allObjects = {
    val iobjects = initialPrepare.txd.dataUpdates.iterator.map(du => du.objectPointer) ++ initialPrepare.txd.refcountUpdates.iterator.map(ru => ru.objectPointer)
    iobjects.foldLeft(Set[ObjectPointer]())( (s, op) => s + op )
  }
  protected var finalized = false
  protected var peerDispositions = Map[DataStoreID, TransactionDisposition.Value]()
  protected var acceptedPeers = Set[DataStoreID]()
  protected var finalizer: Option[TransactionFinalizer] = None
  
  protected def isValidAcceptor(ds: DataStoreID) = ds.poolUUID == initialPrepare.txd.primaryObject.poolUUID && validAcceptorSet.contains(ds.poolIndex)
  
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
            if (committed)  
              finalizer = Some(finalizerFactory.create(initialPrepare.txd, acceptedPeers, messenger))
            else 
              complete(false)
            
            
            onResolution(committed)
          }
        }   
    }
  }
  
  def receiveTxFinalized(msg: TxFinalized): Unit = synchronized { 
    finalized = true
    finalizer.foreach( _.cancel() )
    complete(msg.committed)
  }
  
  def mayBeDiscarded: Boolean = synchronized { finalized }
  
  protected def complete(committed: Boolean) = {
    finalized = true
    onComplete(initialPrepare.txd.transactionUUID)
    
    initialPrepare.txd.originatingClient.foreach(client => {
      messenger.send(client, TxFinalized(storeId, initialPrepare.txd.transactionUUID, committed))
    })
  }
  
  protected def nextRound(): Unit = {
    peerDispositions = Map()
    acceptedPeers = Set()
    proposer.nextRound()
  }
  
  protected def sendAcceptMessages(): Unit = {
    val poolUUID = initialPrepare.txd.primaryObject.poolUUID
    
    val accept = synchronized {
      proposer.currentAcceptMessage().map(paxAccept => 
        (TxAccept(storeId, initialPrepare.txd.transactionUUID, paxAccept.proposalId, paxAccept.proposalValue), acceptedPeers)
      )
    }
    
    accept.foreach(t => {
      initialPrepare.txd.primaryObject.storePointers.foreach(sp => {
        val dest = DataStoreID(poolUUID, sp.poolIndex)
        if (!t._2.contains(dest))
          messenger.send(dest, t._1)
      })
    })
  }
  
  protected def onNack(promisedId: paxos.ProposalID): Unit = {
    proposer.updateHighestProposalId(promisedId)
    nextRound()
  }
  
  protected def onResolution(committed: Boolean): Unit = {}
}

object TransactionDriver {
  trait Factory {
    def create(
        storeId: DataStoreID,
        messenger:TransactionMessenger, 
        initialPrepare: TxPrepare, 
        finalizerFactory: TransactionFinalizer.Factory,
        onComplete: (UUID) => Unit): TransactionDriver
  }
}
