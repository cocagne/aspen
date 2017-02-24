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

class Transaction(val crl: CrashRecoveryLog, val messenger: Messenger, trs: TransactionRecoveryState)(implicit ec: ExecutionContext) {
  val store: DataStore = trs.store
  val txd: TransactionDescription = trs.txd
  val localUpdates: LocalUpdateContent = trs.localUpdates
  
  private[this] var txdisposition: TransactionDisposition.Value = trs.disposition
  private[this] var txstatus: TransactionStatus.Value = trs.status
  
  def disposition: TransactionDisposition.Value = txdisposition
  def status: TransactionStatus.Value = txstatus
  
  private[this] val acceptor = new Acceptor(store.storeId.poolIndex, trs.paxosAcceptorState)
  private[this] val learner = new Learner(txd.primaryObject.ida.width, txd.primaryObject.ida.writeThreshold)
  
  def receive_prepare(prepare: TxPrepare): Unit = {
    
    val response = synchronized {
      acceptor.receivePrepare(Prepare(prepare.proposalId))  
    }
    
    response match {
      case Left(nack) => messenger.send(prepare.from, TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Left(TxPrepareResponse.Nack(nack.promisedProposalId)), 
            prepare.proposalId,
            disposition,
            Nil))
            
      case Right(promise) =>
        
        store.getCurrentObjectState(txd).onSuccess({ case currentState => 
          val errs = checkUpdates(currentState) match {
            case Nil => store.lockOrCollide(txd) match {
              case None => Nil
              
              case Some(collisions) =>
                
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
            }
            case errList => errList
          }
          
          val recoveryState = synchronized {
            txdisposition match {
              case TransactionDisposition.Undetermined => 
                txdisposition = if (errs.isEmpty) TransactionDisposition.VoteCommit else TransactionDisposition.VoteAbort
                
              case TransactionDisposition.VoteAbort => 
                if (errs.isEmpty) 
                  txdisposition = TransactionDisposition.VoteCommit
                
              case TransactionDisposition.VoteCommit =>
            }
            
            TransactionRecoveryState(store, txd, localUpdates, txdisposition, txstatus, acceptor.persistentState)
          }

          val response = TxPrepareResponse(
              store.storeId, 
              txd.transactionUUID, 
              Right(TxPrepareResponse.Promise(recoveryState.paxosAcceptorState.accepted)),
              prepare.proposalId,
              recoveryState.disposition,
              errs)
              
          crl.saveTransactionRecoveryState(recoveryState).onSuccess({case _ => messenger.send(prepare.from, response)})
        })
    }
  }

  private def checkUpdates(currentState: Map[UUID, Either[ObjectError.Value, CurrentObjectState]]): List[UpdateErrorResponse] = {
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
    
    errs
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