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
        
        store.getCurrentObjectState(txd).foreach({  currentState => 
          
          val errs = getUpdateErrors(currentState) match {
            case Some(errs) => Some(errs)
            case None => lockObjectsToTransaction(currentState)
          }
          
          val recoveryState = synchronized {
            
            txdisposition = txdisposition match {
              
              case TransactionDisposition.Undetermined => errs match {
                  case Some(_) => TransactionDisposition.VoteAbort
                  case None => TransactionDisposition.VoteCommit
              }
              
              // We can change our vote from Abort to Commit. An example scenario is two conflicting transactions. If
              // one of them aborts, the conflict will be resolved so the next Prepare message for the remaining
              // transaction will successfully lock our local objects to the transaction, thereby allowing us to
              // change our vote.
              case TransactionDisposition.VoteAbort => errs match { 
                  case Some(_) => TransactionDisposition.VoteAbort
                  case None => TransactionDisposition.VoteCommit
              }
              
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
              errs match {
                case Some(errList) => errList
                case None => Nil
              })
              
          // Note: Saving the state can fail and in that event we cannot send the message since it would violate the
          //       Paxos safety requirements. Logging the failure here probably isn't a good idea as it is likely 
          //       that the node will be in this state for a significant period of time. Leave it to the 
          //       CrashRecoveryHandler to do the appropriate logging. In the mean time, we should still do
          //       everything normally. We'll just be a non-voting Transaction participant
          crl.saveTransactionRecoveryState(recoveryState).foreach(_ => messenger.send(response))
        })
    }
  }

  private[this] def getUpdateErrors(currentState: Map[UUID, Either[ObjectError.Value, CurrentObjectState]]): Option[List[UpdateErrorResponse]] = {
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
  
  private[this] def lockObjectsToTransaction(currentState: Map[UUID, Either[ObjectError.Value, CurrentObjectState]]): Option[List[UpdateErrorResponse]] = { 
    store.lockOrCollide(txd).map(collisions => {  
      val duerrs = txd.dataUpdates.zipWithIndex.foldLeft(List[UpdateErrorResponse]())((l, tpl) => collisions.get(tpl._1.objectPointer.uuid) match {
        case None => l
        case Some(err) => 
          val e = err match {
            case Left(objErr) => UpdateErrorResponse(UpdateType.Data, tpl._2.toByte, objectErrorToUpdateError(objErr), None, None, None)
            case Right(collidingTxd) => UpdateErrorResponse(UpdateType.Data, tpl._2.toByte, UpdateError.Collision, None, None, Some(collidingTxd))
          }
          e :: l
      })
      
      val ruerrs = txd.refcountUpdates.zipWithIndex.foldLeft(List[UpdateErrorResponse]())((l, tpl) => collisions.get(tpl._1.objectPointer.uuid) match {
        case None => l
        case Some(err) =>
          val e = err match {
            case Left(objErr) => UpdateErrorResponse(UpdateType.Refcount, tpl._2.toByte, objectErrorToUpdateError(objErr), None, None, None)
            case Right(collidingTxd) => UpdateErrorResponse(UpdateType.Refcount, tpl._2.toByte, UpdateError.Collision, None, None, Some(collidingTxd))
          }
          e :: l
      })
      
      duerrs ++ ruerrs
    })
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
  
  def objectErrorToUpdateError(objErr: ObjectError.Value): UpdateError.Value = objErr match {
    case ObjectError.InvalidLocalPointer => UpdateError.InvalidLocalPointer
    case ObjectError.ObjectMismatch => UpdateError.ObjectMismatch
    case ObjectError.CorruptedObject => UpdateError.CorruptedObject
    case ObjectError.RevisionMismatch => UpdateError.RevisionMismatch
    case ObjectError.RefcountMismatch => UpdateError.RefcountMismatch
  }
}