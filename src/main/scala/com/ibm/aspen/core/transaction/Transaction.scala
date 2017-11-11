package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.data_store.DataStore
import com.ibm.aspen.core.transaction.paxos.PersistentState
import com.ibm.aspen.core.transaction.paxos.Acceptor
import com.ibm.aspen.core.transaction.paxos.Learner
import com.ibm.aspen.core.transaction.paxos.Prepare
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import java.util.UUID
import com.ibm.aspen.core.data_store.CurrentObjectState
import com.ibm.aspen.core.data_store.ObjectError
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.transaction.paxos.Accept
import com.ibm.aspen.core.transaction.paxos.Accepted
import scala.concurrent.Future
import java.nio.ByteBuffer
import com.ibm.aspen.core.data_store.ObjectError
import com.ibm.aspen.core.data_store.ObjectError
import com.ibm.aspen.core.data_store.InvalidLocalPointer
import com.ibm.aspen.core.data_store.CorruptedObject
import com.ibm.aspen.core.data_store.ObjectMismatch
import com.ibm.aspen.core.data_store.ObjectReadError
import com.ibm.aspen.core.data_store.RevisionMismatch
import com.ibm.aspen.core.data_store.ObjectError
import com.ibm.aspen.core.data_store.RefcountMismatch
import com.ibm.aspen.core.data_store.ObjectTransactionError
import com.ibm.aspen.core.data_store.TransactionReadError
import com.ibm.aspen.core.data_store.TransactionCollision

class Transaction(
    val crl: CrashRecoveryLog, 
    val messenger: StoreSideTransactionMessenger,
    val onDiscard: (Transaction) => Unit,
    val store: DataStore,
    trs: TransactionRecoveryState)(implicit ec: ExecutionContext) {
  
  val txd: TransactionDescription = trs.txd
  val localUpdates: Option[Array[ByteBuffer]] = trs.localUpdates
  
  private[this] var txdisposition: TransactionDisposition.Value = trs.disposition
  private[this] var commitFuture: Option[Future[Unit]] = None
  
  private[this] val acceptor = new Acceptor(store.storeId.poolIndex, trs.paxosAcceptorState)
  private[this] val learner = new Learner(txd.primaryObject.ida.width, txd.primaryObject.ida.writeThreshold)
  
  // NOTE: Call these methods only within synchronized{} blocks
  private[this] def transactionStatus = learner.finalValue match {
    case None => TransactionStatus.Unresolved
    case Some(committed) => if (committed) TransactionStatus.Committed else TransactionStatus.Aborted
  }
  private[this] var resolved = false
  
  import Transaction._
  
  /*
   *  private[this] def getUpdateErrors(currentState: Map[UUID, Either[ObjectError.Value, CurrentObjectState]]): Option[List[UpdateErrorResponse]] = {
    var errs: List[UpdateErrorResponse] = Nil

    def convertErr(e: ObjectError.Value) = e match {
      case ObjectError.InvalidLocalPointer => UpdateError.InvalidLocalPointer
      case ObjectError.CorruptedObject => UpdateError.CorruptedObject
      case ObjectError.ObjectMismatch => UpdateError.ObjectMismatch
    }

    txd.dataUpdates.zipWithIndex.foreach(t => currentState.get(t._1.objectPointer.uuid).foreach( s => s match {
      case Left(err) =>
        val (du, updateIndex) = t

        errs = UpdateErrorResponse(UpdateType.Data, updateIndex.toByte, convertErr(err), None, None, None) :: errs

      case Right(cs) =>
        val (du, updateIndex) = t

        if ( !(localUpdates.isDefined && localUpdates.get.size > updateIndex) )
          errs = UpdateErrorResponse(UpdateType.Data, updateIndex.toByte, UpdateError.MissingUpdateData, None, None, None) :: errs

        if (cs.revision != du.requiredRevision)
          errs = UpdateErrorResponse(UpdateType.Data, updateIndex.toByte, UpdateError.RevisionMismatch, Some(cs.revision), None, None) :: errs
    }))

    txd.refcountUpdates.zipWithIndex.foreach(t => currentState.get(t._1.objectPointer.uuid).foreach( s => s match {
      case Left(err) =>
        val (ru, updateIndex) = t

        errs = UpdateErrorResponse(UpdateType.Refcount, updateIndex.toByte, convertErr(err), None, None, None) :: errs

      case Right(cs) =>
        val (ru, updateIndex) = t

        if (cs.refcount != ru.requiredRefcount)
          errs = UpdateErrorResponse(UpdateType.Refcount, updateIndex.toByte, UpdateError.RefcountMismatch, None, Some(cs.refcount), None) :: errs
    }))

    if (errs.isEmpty)
      None
    else
      Some(errs.reverse)
  }
   */
  
  def receivePrepare(prepare: TxPrepare): Unit = {
    
    val (response, acceptorState, originalDisposition) = synchronized {
       (acceptor.receivePrepare(Prepare(prepare.proposalId)), acceptor.persistentState, txdisposition)
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
            
      case Right(promise) =>
        /* 
         * Refactor lockTransaction to take the localUpdates: Option
         * 
         * Refactor tx update error checking function into a standard method on the DataStore trait
         * that takes the DataUpdate/RefcountUpdate info plus the current object state and optionally returns
         * a TransactionError
         * 
         * Then have mem & rocks use this.
         * 
         * Need to detect missing local updates and error when it happens
         * 
         */
        store.lockTransaction(txd).foreach { errors =>
          
          val recoveryState = synchronized {
            
            txdisposition = txdisposition match {
              
              case TransactionDisposition.Undetermined => if (errors.isEmpty) 
                TransactionDisposition.VoteCommit else  TransactionDisposition.VoteAbort
              
              // We can change our vote from Abort to Commit. An example scenario is two conflicting transactions. If
              // one of them aborts, the conflict will be resolved so the next Prepare message for the remaining
              // transaction will successfully lock our local objects to the transaction, thereby allowing us to
              // change our vote.
              case TransactionDisposition.VoteAbort => if (errors.isEmpty) 
                TransactionDisposition.VoteCommit else  TransactionDisposition.VoteAbort
              
              // Once we've voted to commit, we're committed to committing :-)
              case TransactionDisposition.VoteCommit => TransactionDisposition.VoteCommit
            }
            
            TransactionRecoveryState(store.storeId, txd, localUpdates, txdisposition, transactionStatus, acceptorState)
          }

          val response = TxPrepareResponse(
              prepare.from,
              store.storeId, 
              txd.transactionUUID, 
              Right(TxPrepareResponse.Promise(recoveryState.paxosAcceptorState.accepted)),
              prepare.proposalId,
              recoveryState.disposition,
              errors.map(createUpdateErrorResponse))
              
          // Note: Saving the state can fail and in that event we cannot send the message since it would violate the
          //       Paxos safety requirements. Logging the failure here probably isn't a good idea as it is likely 
          //       that the node will be in this state for a significant period of time. Leave it to the 
          //       CrashRecoveryLog to do the appropriate logging. In the mean time, we should still do
          //       everything normally. We'll just be a non-voting Transaction participant
          crl.saveTransactionRecoveryState(recoveryState).foreach(_ => messenger.send(response))
        }
    }
  }

  
  def receiveAccept(msg: TxAccept): Unit = {
    val (paxosReply, acceptorState) = synchronized { 
      (acceptor.receiveAccept(Accept(msg.proposalId, msg.value)), acceptor.persistentState) 
    } 
    
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
        val recoveryState = synchronized {
          TransactionRecoveryState(store.storeId, txd, localUpdates, txdisposition, transactionStatus, acceptorState)
        }

        val response = TxAcceptResponse(
            msg.from,
            store.storeId, 
            txd.transactionUUID, 
            msg.proposalId,
            Right(TxAcceptResponse.Accepted(accept.proposalValue)))
            
        // Note: For the reasons explained in the receive_prepare() comment, ignore errors here as well
        crl.saveTransactionRecoveryState(recoveryState).foreach{ _ => 
          messenger.send(response)
          txd.originatingClient.foreach( client => messenger.send(client, response) )
        }
    }
  }
  
  def receiveAcceptResponse(msg: TxAcceptResponse): Option[Boolean] = msg.response match {
    case Left(nack) => None
    
    case Right(accepted) => synchronized {
      val previouslyResolved = learner.finalValue.isDefined
      
      learner.receiveAccepted(Accepted(msg.from.poolIndex, msg.proposalId, accepted.value))
      
      if (!previouslyResolved && learner.finalValue.isDefined) 
        onResolution(learner.finalValue.get)
      
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
  }
  
  def receiveFinalized(msg: TxFinalized): Unit = synchronized {
    // My have missed learning of commit via Paxos and TxResolved
    onResolution(msg.committed)
    
    // If transaction committed (which it must have in order for a TxFinalized to be received...) discard
    // transaction state once our commit operation is complete
    commitFuture.foreach(_ => discardTransactionState())
  }
  
  private[this] def discardTransactionState(): Unit = {
    crl.discardTransactionState(txd)
    store.discardTransaction(txd)
    onDiscard(this)
  }
}

object Transaction {
  def apply(
      crl: CrashRecoveryLog,
      messenger: StoreSideTransactionMessenger,
      onDiscard: (Transaction) => Unit,
      store: DataStore, 
      txd: TransactionDescription, 
      localUpdates: Option[Array[ByteBuffer]])(implicit ec: ExecutionContext): Transaction = {
    new Transaction(crl, messenger, onDiscard, store, TransactionRecoveryState(
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
      onDiscard: (Transaction) => Unit,
      store: DataStore,
      trs: TransactionRecoveryState)(implicit ec: ExecutionContext) = new Transaction(crl, messenger, onDiscard, store, trs)
  
  def createUpdateErrorResponse(txErr: ObjectTransactionError): UpdateErrorResponse = txErr match {
    case e: TransactionReadError => e.kind match {
      case r: InvalidLocalPointer => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.InvalidLocalPointer, None, None, None)
      case r: ObjectMismatch      => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.ObjectMismatch, None, None, None)
      case r: CorruptedObject     => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.CorruptedObject, None, None, None)
    }
    case e: RevisionMismatch      => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.RevisionMismatch, Some(e.current), None, None)
    case e: RefcountMismatch      => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.RefcountMismatch, None, Some(e.current), None)
    case e: TransactionCollision  => UpdateErrorResponse(e.objectPointer.uuid, UpdateError.Collision, None, None, Some(e.lockedTransaction))
  }
  
}