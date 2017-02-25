package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.data_store.DataStore
import com.ibm.aspen.core.transaction.paxos.PersistentState
import com.ibm.aspen.core.transaction.paxos.Acceptor
import com.ibm.aspen.core.transaction.paxos.Learner
import com.ibm.aspen.core.transaction.paxos.Prepare
import com.ibm.aspen.core.network.Messenger
import java.util.UUID
import com.ibm.aspen.core.data_store.CurrentObjectState
import com.ibm.aspen.core.data_store.ObjectError
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.transaction.paxos.Accept
import com.ibm.aspen.core.transaction.paxos.Accepted
import scala.concurrent.Future

class Transaction(val crl: CrashRecoveryLog, val messenger: Messenger, trs: TransactionRecoveryState)(implicit ec: ExecutionContext) {
  val store: DataStore = trs.store
  val txd: TransactionDescription = trs.txd
  val localUpdates: LocalUpdateContent = trs.localUpdates
  
  private[this] var txdisposition: TransactionDisposition.Value = trs.disposition
  private[this] var txstatus: TransactionStatus.Value = trs.status
  private[this] var commitFuture: Option[Future[Unit]] = None
  
  def disposition: TransactionDisposition.Value = txdisposition
  def status: TransactionStatus.Value = txstatus
  
  private[this] val acceptor = new Acceptor(store.storeId.poolIndex, trs.paxosAcceptorState)
  private[this] val learner = new Learner(txd.primaryObject.ida.width, txd.primaryObject.ida.writeThreshold)
  
  def receivePrepare(prepare: TxPrepare): Unit = {
    
    val (response, acceptorState) = synchronized {
       (acceptor.receivePrepare(Prepare(prepare.proposalId)), acceptor.persistentState)
    }
    
    response match {
      case Left(nack) => 
        val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Left(TxPrepareResponse.Nack(nack.promisedProposalId)), 
            prepare.proposalId,
            disposition,
            Nil)
            
        messenger.send(prepare.from, response)
            
      case Right(promise) =>
        
        store.getCurrentObjectState(txd).onSuccess({ case currentState => 
          
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
            
            TransactionRecoveryState(store, txd, localUpdates, txdisposition, txstatus, acceptorState)
          }

          val response = TxPrepareResponse(
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
          crl.saveTransactionRecoveryState(recoveryState, Some(localUpdates)).onSuccess({case _ => messenger.send(prepare.from, response)})
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
        
        if (!localUpdates.haveDataForUpdateIndex(updateIndex))
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
        case Some(collidingTxd) => 
          val e = UpdateErrorResponse(UpdateType.Data, tpl._2.toByte, UpdateError.Collision, None, None, Some(collidingTxd))
          e :: l
      })
      
      val ruerrs = txd.refcountUpdates.zipWithIndex.foldLeft(List[UpdateErrorResponse]())((l, tpl) => collisions.get(tpl._1.objectPointer.uuid) match {
        case None => l
        case Some(collidingTxd) => 
          val e = UpdateErrorResponse(UpdateType.Refcount, tpl._2.toByte, UpdateError.Collision, None, None, Some(collidingTxd))
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
            store.storeId, 
            txd.transactionUUID, 
            msg.proposalId,
            Left(TxAcceptResponse.Nack(nack.promisedProposalId)))
            
        messenger.send(msg.from, response)
        
      case Right(accept) =>
        val recoveryState = synchronized {
          TransactionRecoveryState(store, txd, localUpdates, txdisposition, txstatus, acceptorState)
        }

        val response = TxAcceptResponse(
            store.storeId, 
            txd.transactionUUID, 
            msg.proposalId,
            Right(TxAcceptResponse.Accepted(accept.proposalValue)))
            
        // Note: For the reasons explained in the receive_prepare() comment, ignore errors here as well
        crl.saveTransactionRecoveryState(recoveryState, None).onSuccess({case _ => messenger.send(msg.from, response)})
    }
  }
  
  def receiveAcceptResponse(msg: TxAcceptResponse): Unit = msg.response match {
    case Left(nack) => // Nothing to do
    case Right(accepted) =>
      
      synchronized {
        learner.receiveAccepted(Accepted(msg.from.poolIndex, msg.proposalId, accepted.value))
        
        if (txstatus == TransactionStatus.Unresolved && learner.finalValue.isDefined) {
          txstatus = if (learner.finalValue.get) {
            commitFuture = Some(store.commitTransactionUpdates(txd, localUpdates))
            TransactionStatus.Committed 
          }
          else {
            crl.discardTransactionState(txd)
            TransactionStatus.Aborted
          }
        }
      }
  }
  
  def receiveFinalized(msg: TxFinalized): Unit = synchronized {
    
    // If we missed some of the AcceptResponse messages, we may see the Finalized message before realizing
    // that the commit decision was made. If so, we'll want to commit our local changes before discarding 
    // the transaction state
    if (txstatus == TransactionStatus.Unresolved) {
      txstatus = if (learner.finalValue.get) {
        commitFuture = Some(store.commitTransactionUpdates(txd, localUpdates))
        TransactionStatus.Committed 
      }
      else 
        TransactionStatus.Aborted 
    }
  
    commitFuture match {
      case Some(f) => f onSuccess({case _ => crl.discardTransactionState(txd)})
      case None => crl.discardTransactionState(txd)
    }
  }
    
  
}

object Transaction {
  def apply(
      crl: CrashRecoveryLog,
      messenger: Messenger, 
      store: DataStore, 
      txd: TransactionDescription, 
      localUpdates: LocalUpdateContent)(implicit ec: ExecutionContext): Transaction = {
    new Transaction(crl, messenger, TransactionRecoveryState(
        store,
        txd, 
        localUpdates, 
        TransactionDisposition.Undetermined, 
        TransactionStatus.Unresolved, 
        PersistentState(None, None)))
  }
  
  def apply(
      crl: CrashRecoveryLog, 
      messenger: Messenger, 
      trs: TransactionRecoveryState)(implicit ec: ExecutionContext) = new Transaction(crl, messenger, trs)
}