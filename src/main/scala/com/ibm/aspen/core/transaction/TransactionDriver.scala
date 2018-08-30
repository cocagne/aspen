package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.Proposer
import com.ibm.aspen.core.transaction.paxos.Learner
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.transaction.paxos.ProposalID

abstract class TransactionDriver(
    val storeId: DataStoreID,
    val messenger: StoreSideTransactionMessenger, 
    val txd: TransactionDescription, 
    private val finalizerFactory: TransactionFinalizer.Factory,
    private val onComplete: (UUID) => Unit)(implicit ec: ExecutionContext) {
  
  import TransactionDriver._
  
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
  
  def shutdown(): Unit = {}
  
  def heartbeat(): Unit = messenger.send(allDataStores.map(toStoreId => TxHeartbeat(toStoreId, storeId, txd.transactionUUID)))
      
  def receiveTxPrepare(msg: TxPrepare): Unit = synchronized {
      proposer.updateHighestProposalId(msg.proposalId)
  }
  
  
  def receiveTxPrepareResponse(msg: TxPrepareResponse, txresult: (UUID) => Option[Boolean]): Unit = synchronized {
    
    if (msg.proposalId != proposer.currentProposalId)
      return
      
//    if (!msg.errors.isEmpty) {
//      msg.errors.foreach( err => println(s"C  ${msg.from.poolIndex} TxErr ${txd.transactionUUID} $err") )
//    }
      
    // If the response says there is a transaction collision with a transaction we know has successfully completed,
    // there's a race condition. Drop the response and re-send the prepare. Eventually it will either be successfully
    // processed or a different error will occur
    val collisionsWithCompletedTransactions = !msg.errors.isEmpty && msg.errors.forall { e =>
      e.conflictingTransaction match {
        case None => false
        case Some((txuuid, txts)) => txresult(txuuid) match { 
          case None => false
          case Some(b) => b
        }
      }
    }
    
    if (collisionsWithCompletedTransactions) {
      // Race condition. Drop this message and re-send the prepare
      sendPrepareMessage(msg.from)
      return
    }
      
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
            var nAbortVotes = 0
            var nCommitVotes = 0
            var canCommitObject = true
            
            op.storePointers.foreach(sp => peerDispositions.get(DataStoreID(op.poolUUID, sp.poolIndex)).foreach(disposition => {
              nReplies += 1
              disposition match {
                case TransactionDisposition.VoteCommit => nCommitVotes += 1
                case _ => nAbortVotes += 1
                  // TODO: We can be quite a bit smarter about choosing when we must abort
                  //canCommitObject = false
              }
            }))
            
            if (nReplies < op.ida.writeThreshold)
              return
            
            if (nReplies != op.ida.width && nCommitVotes < op.ida.writeThreshold && op.ida.width - nAbortVotes >= op.ida.writeThreshold)
              return
              
            if (op.ida.width - nAbortVotes < op.ida.writeThreshold)
              canCommitObject = false
              
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
          case Some(committed) =>
            
            if (!alreadyResolved) {
              if (committed) {

                val f = finalizerFactory.create(txd, messenger)
                
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
  
  def receiveTxCommitted(msg: TxCommitted): Unit = synchronized {
    finalizer.foreach { f =>
      f.updateCommittedPeer(msg.from)
    }
  }
  
  def receiveTxFinalized(msg: TxFinalized): Unit = synchronized { 
    finalizer.foreach( _.cancel() )
    onFinalized(msg.committed)
  }
  
  def mayBeDiscarded: Boolean = synchronized { finalized }
  
  protected def onFinalized(committed: Boolean): Unit = synchronized {
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
  
  protected def sendPrepareMessages(): Unit = {
    val proposalId = synchronized { proposer.currentProposalId }
    
    txd.allDataStores.foreach( toStore => messenger.send(TxPrepare(toStore, storeId, txd, proposalId)) )
  }
  
  protected def sendPrepareMessage(storeId: DataStoreID): Unit = {
    val proposalId = synchronized { proposer.currentProposalId }
    
    messenger.send(TxPrepare(storeId, storeId, txd, proposalId))
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
        txd: TransactionDescription, 
        finalizerFactory: TransactionFinalizer.Factory,
        onComplete: (UUID) => Unit)(implicit ec: ExecutionContext): TransactionDriver
  }
  
  object noErrorRecoveryFactory extends Factory {
    class NoRecoveryTransactionDriver(
        storeId: DataStoreID,
        messenger: StoreSideTransactionMessenger, 
        txd: TransactionDescription, 
        finalizerFactory: TransactionFinalizer.Factory,
        onComplete: (UUID) => Unit)(implicit ec: ExecutionContext) extends TransactionDriver(storeId, messenger, txd, finalizerFactory, onComplete) 
    
    def create(
        storeId: DataStoreID,
        messenger:StoreSideTransactionMessenger, 
        txd: TransactionDescription, 
        finalizerFactory: TransactionFinalizer.Factory,
        onComplete: (UUID) => Unit)(implicit ec: ExecutionContext): TransactionDriver = {
      new NoRecoveryTransactionDriver(storeId, messenger, txd, finalizerFactory, onComplete)
    }
  }
}
