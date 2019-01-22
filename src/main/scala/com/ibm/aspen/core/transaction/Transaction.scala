package com.ibm.aspen.core.transaction

import java.util.UUID

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.data_store._
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.transaction.paxos._

import scala.concurrent.{ExecutionContext, Future}

class Transaction(
    val crl: CrashRecoveryLog, 
    val messenger: StoreSideTransactionMessenger,
    onCommit: List[UUID] => Unit,
    val onDiscard: Transaction => Unit,
    val store: DataStore,
    trs: TransactionRecoveryState)(implicit ec: ExecutionContext) {
  
  import Transaction._
  
  val txd: TransactionDescription = trs.txd

  private[this] val acceptor = new Acceptor(store.storeId.poolIndex, trs.paxosAcceptorState)
  private[this] val learner = new Learner(txd.primaryObject.ida.width, txd.primaryObject.ida.writeThreshold)

  private[this] var localUpdates: Option[List[LocalUpdate]] = trs.localUpdates
  private[this] var preTxRebuilds: List[PreTransactionOpportunisticRebuild] = Nil
  private[this] var txdisposition: TransactionDisposition.Value = trs.disposition
  private[this] var commitFuture: Option[Future[List[UUID]]] = None

  private[this] var resolved = false
  private[this] var discarded = false
  
  private[this] var heartbeatTimestamp: Long = 0

  def preTransactionRebuilds: List[PreTransactionOpportunisticRebuild] = synchronized(preTxRebuilds)

  private[this] def transactionStatus: TransactionStatus.Value = learner.finalValue match {
    case None => TransactionStatus.Unresolved
    case Some(committed) => if (committed) TransactionStatus.Committed else TransactionStatus.Aborted
  }
 
  private def updateTimestamp(): Unit = synchronized { heartbeatTimestamp = System.currentTimeMillis() }
  
  def lastHeartbeat: Long = synchronized { heartbeatTimestamp }
  
  def heartbeatReceived(): Unit = updateTimestamp()
  
  def currentTransactionStatus(): TransactionStatus.Value = synchronized { transactionStatus }
  
  def receivePrepare(prepare: TxPrepare, otxData: Option[TransactionData]): Unit = {

    // Proposal ID 1 is always sent by the client initiating the transaction. We don't want to update
    // the timestamp for this since the client can't drive the transaction to completion and it'll
    // continually re-transmit the request to work around connection issues. Prepares sent by stores,
    // which will use a proposalId > 1 should up the the timestamp so we don't time out and also
    // attempt to drive the transaction forward.
    if (prepare.proposalId.number != 1)
      updateTimestamp()

    val (response, acceptorState, originalDisposition, dataUpdates) = synchronized {
      otxData.foreach { txData =>
        if (localUpdates.isEmpty && txData.localUpdates.nonEmpty)
          localUpdates = Some(txData.localUpdates)

        if (preTxRebuilds.isEmpty && txData.preTransactionRebuilds.nonEmpty)
          preTxRebuilds = txData.preTransactionRebuilds
      }

      (acceptor.receivePrepare(Prepare(prepare.proposalId)), acceptor.persistentState, txdisposition, localUpdates)
    }
    
    response match {
      case Left(nack) => 
        val response = TxPrepareResponse(
            prepare.from,
            store.storeId, 
            txd.transactionUUID, 
            Left(TxPrepareResponse.Nack(nack.promisedProposalId)), 
            prepare.proposalId,
            originalDisposition,
            Nil)
            
        messenger.send(response)
            
      case Right(_) =>
        
        store.lockTransaction(txd, dataUpdates).foreach { errors => synchronized {
          
          if (discarded && errors.isEmpty) {
            // It's possible for a slow load & lock operation to return long after the transaction has completed
            // and been discarded. If so, we'll simply ignore the message and release the lock if it was acquired.
            store.discardTransaction(txd)
            
          } else {
            
            txdisposition = txdisposition match {
              
              case TransactionDisposition.Undetermined =>
                if (errors.isEmpty) TransactionDisposition.VoteCommit else TransactionDisposition.VoteAbort

              case TransactionDisposition.VoteAbort =>
                // We can change our vote from Abort to Commit. An example scenario is two conflicting transactions. If
                // one of them aborts, the conflict will be resolved so the next Prepare message for the remaining
                // transaction will successfully lock our local objects to the transaction, thereby allowing us to
                // change our vote.
                if (errors.isEmpty) TransactionDisposition.VoteCommit else TransactionDisposition.VoteAbort
              

              case TransactionDisposition.VoteCommit =>
                // Once we've voted to commit, we're committed to committing :-)
                TransactionDisposition.VoteCommit
            }
              
            val recoveryState = TransactionRecoveryState(store.storeId, txd, localUpdates, txdisposition,
                                                         transactionStatus, acceptorState)

            val response = TxPrepareResponse(
                prepare.from,
                store.storeId, 
                txd.transactionUUID, 
                Right(TxPrepareResponse.Promise(recoveryState.paxosAcceptorState.accepted)),
                prepare.proposalId,
                recoveryState.disposition,
                errors.map(createUpdateErrorResponse))
            
            // Note: Saving the state can fail and if it does we cannot send the message as it would violate the
            //       Paxos safety requirements. Logging the failure here probably isn't a good idea as it is likely 
            //       that the node will be in this state for a significant period of time. Leave it to the 
            //       CrashRecoveryLog to do the appropriate logging. In the mean time, we should still do
            //       everything normally. We'll just be a non-voting Transaction participant
            crl.saveTransactionRecoveryState(recoveryState).foreach { _ => 
              messenger.send(response)
              txd.originatingClient.foreach( client => messenger.send(client, response) )
            }
          }
        }}
    }
  }

  
  def receiveAccept(msg: TxAccept): Unit = synchronized {

    if (discarded)
      return // old message

    updateTimestamp()

    val paxosReply = acceptor.receiveAccept(Accept(msg.proposalId, msg.value))
    val acceptorState = acceptor.persistentState

    paxosReply match {
      
      case Left(nack) =>
        val response = TxAcceptResponse(
            msg.from,
            store.storeId, 
            txd.transactionUUID, 
            msg.proposalId,
            Left(TxAcceptResponse.Nack(nack.promisedProposalId)))
            
        messenger.send(response)
        txd.originatingClient.foreach( client => messenger.send(client, response) )
        
      case Right(accept) =>

        val recoveryState = TransactionRecoveryState(store.storeId, txd, localUpdates, txdisposition,
          transactionStatus, acceptorState)

        val response = TxAcceptResponse(
          msg.from,
          store.storeId,
          txd.transactionUUID,
          msg.proposalId,
          Right(TxAcceptResponse.Accepted(accept.proposalValue)))

        // Note: For the reasons explained in the receive_prepare() comment, ignore errors here as well
        crl.saveTransactionRecoveryState(recoveryState).foreach { _ =>
          messenger.send(response)
          txd.originatingClient.foreach(client => messenger.send(client, response))
        }
    }
  }
  
  def receiveAcceptResponse(msg: TxAcceptResponse): Option[Boolean] = synchronized {
    msg.response match {
      case Left(_) => None

      case Right(accepted) =>

        learner.receiveAccepted(Accepted(msg.from.poolIndex, msg.proposalId, accepted.value))

        learner.finalValue.foreach(onResolution)

        learner.finalValue
    }
  }
  
  // Must be called from within a synchronized block
  protected def onResolution(txCommitted: Boolean): Unit = if (!resolved) {
    resolved = true
    
    if (txCommitted)
      commitFuture = Some(store.commitTransactionUpdates(txd, localUpdates))
    else
      discardTransactionState()
  }
  
  def receiveResolved(msg: TxResolved): Unit = synchronized {
    // May learn of successful commit from TransactionDriver rather than by way of Paxos
    onResolution(msg.committed)
    
    if (msg.committed) {
      commitFuture.foreach { fcommit =>
        fcommit.foreach { objectCommitErrors =>
          onCommit(objectCommitErrors)
          messenger.send(TxCommitted(msg.from, store.storeId, txd.transactionUUID, objectCommitErrors))
        }
      }
    }
  }
  
  def receiveFinalized(msg: TxFinalized): Unit = synchronized {
    // My have missed learning of commit via Paxos and TxResolved
    onResolution(msg.committed)
    
    // If transaction committed (which it must have in order for a TxFinalized to be received...) discard
    // transaction state once our commit operation is complete
    commitFuture.foreach(_ => discardTransactionState())
  }
  
  private[this] def discardTransactionState(): Unit = if (!discarded) {
    discarded = true
    crl.discardTransactionState(store.storeId, txd)
    store.discardTransaction(txd)
    onDiscard(this)
  }
}

object Transaction {

  def apply(
      crl: CrashRecoveryLog,
      messenger: StoreSideTransactionMessenger,
      onCommit: List[UUID] => Unit,
      onDiscard: Transaction => Unit,
      store: DataStore, 
      txd: TransactionDescription,
      localUpdates: Option[List[LocalUpdate]])(implicit ec: ExecutionContext): Transaction = {
    new Transaction(crl, messenger, onCommit, onDiscard, store, TransactionRecoveryState(
        store.storeId,
        txd,
        localUpdates,
        TransactionDisposition.Undetermined, 
        TransactionStatus.Unresolved, 
        PersistentState(None, None)))
  }
  
  def apply(
      crl: CrashRecoveryLog, 
      messenger: StoreSideTransactionMessenger,
      onCommit: List[UUID] => Unit,
      onDiscard: Transaction => Unit,
      store: DataStore,
      trs: TransactionRecoveryState)(implicit ec: ExecutionContext): Transaction ={

    new Transaction(crl, messenger, onCommit, onDiscard, store, trs)

  }
  
  def createUpdateErrorResponse(txErr: ObjectTransactionError): UpdateErrorResponse = txErr match {
    case e: TransactionReadError => e.kind match {
      case _: InvalidLocalPointer    => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.InvalidLocalPointer, None, None, None)
      case _: ObjectMismatch         => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.ObjectMismatch, None, None, None)
      case _: CorruptedObject        => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.CorruptedObject, None, None, None)
    }                                
    case e: RevisionMismatch         => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.RevisionMismatch, Some(e.current), None, None)
    case e: RefcountMismatch         => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.RefcountMismatch, None, Some(e.current), None)
    case e: TransactionCollision     => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.TransactionCollision, None, None,
                                                            Some((e.lockedTransaction.transactionUUID, HLCTimestamp(e.lockedTransaction.startTimestamp))))
    case e: RebuildCollision         => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.RebuildCollision, None, None, None)
    case e: MissingUpdateContent     => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.MissingUpdateData, None, None, None)
    case e: InsufficientFreeSpace    => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.InsufficientFreeSpace, None, None, None)
    case e: InvalidObjectType        => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.InvalidObjectType, None, None, None)
    case e: KeyValueRequirementError => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.KeyValueRequirementError, None, None, None)
    case e: TransactionTimestampError => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.TransactionTimestampError, None, None, None)
  }
  
}