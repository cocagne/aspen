package com.ibm.aspen.core.transaction

import scala.concurrent._
import ExecutionContext.Implicits.global
import org.scalatest._
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.ProposalID
import com.ibm.aspen.core.data_store.NullDataStore
import com.ibm.aspen.core.network.NullMessenger
import com.ibm.aspen.core.crl.NullCRL
import com.ibm.aspen.core.transaction.paxos.PersistentState
import java.util.UUID
import com.ibm.aspen.core.data_store.ObjectError
import com.ibm.aspen.core.data_store.CurrentObjectState

object TransactionSuite {
  
  val poolUUID = java.util.UUID.randomUUID()
  
  def mkobj = ObjectPointer(java.util.UUID.randomUUID(), poolUUID, None, Replication(3,2), new Array[StorePointer](0))
  
  def mktxd(du: List[DataUpdate], ru: List[RefcountUpdate]) = TransactionDescription(
      java.util.UUID.randomUUID(), 100, mkobj, 0, du, ru, Nil)
      
  def mkprep(paxosRound: Int, fromPeer: Byte, txd: TransactionDescription) = TxPrepare(DataStoreID(poolUUID,fromPeer), txd, ProposalID(paxosRound,fromPeer))
  
  object HaveContent extends LocalUpdateContent {
    def haveDataForUpdateIndex(updateIndex: Int): Boolean = true
  }
  
  object LackContent extends LocalUpdateContent {
    def haveDataForUpdateIndex(updateIndex: Int): Boolean = false
  }
  
  class TMessenger extends NullMessenger {
    var p = Promise[(DataStoreID,Message)]()
    
    def futureMessage = p.future
    
    override def send(toStore: DataStoreID, message: Message): Unit = {
      val t = p
      p = Promise[(DataStoreID,Message)]()
      t success (toStore, message)
    }
  }
}

class TransactionSuite  extends AsyncFunSuite with Matchers {
  import TransactionSuite._

  test("Nack prepare message") {
    val store = new NullDataStore(DataStoreID(poolUUID,0))
    val messenger = new TMessenger
    val crl = new NullCRL
    val txd = mktxd(Nil, Nil)
    
    val promisedId = ProposalID(5,1)
    val tx = new Transaction(crl, messenger, TransactionRecoveryState(
        store, txd, HaveContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(Some(promisedId), None)))
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Left(TxPrepareResponse.Nack(promisedId)), 
            ProposalID(1,2),
            TransactionDisposition.Undetermined,
            Nil)
            
    futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 2), response)) }
	}
  
  test("Simple vote commit") {
    val store = new NullDataStore(DataStoreID(poolUUID,0))
    val messenger = new TMessenger
    val crl = new NullCRL
    val txd = mktxd(Nil, Nil)
    
    val tx = Transaction(crl, messenger, store, txd, HaveContent)
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,2),
            TransactionDisposition.VoteCommit,
            Nil)
            
    futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 2), response)) }
	}
  
  test("Verify state persisted before message send") {
    var stateSavedBeforeMessageSent = false
    var crlStateSaved = false
    
    val store = new NullDataStore(DataStoreID(poolUUID,0))
    val crl = new NullCRL {
      override def saveTransactionRecoveryState(state: TransactionRecoveryState, dataUpdateContent: Option[LocalUpdateContent]): Future[Unit] = {
        crlStateSaved = true
        super.saveTransactionRecoveryState(state, dataUpdateContent)
      }
    }
    val messenger = new TMessenger {
      override def send(toStore: DataStoreID, message: Message): Unit = {
        stateSavedBeforeMessageSent = crlStateSaved
        super.send(toStore, message)
      }
    }
    
    val txd = mktxd(Nil, Nil)
    
    val tx = Transaction(crl, messenger, store, txd, HaveContent)
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,2),
            TransactionDisposition.VoteCommit,
            Nil)
            
    futureResponse map { 
      msg => 
        msg should be ((DataStoreID(poolUUID, 2), response))
        stateSavedBeforeMessageSent should be (true)
    }
	}
  
  test("Vote commit with updates") {
    val store = new NullDataStore(DataStoreID(poolUUID,0))
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
    
    val tx = Transaction(crl, messenger, store, txd, HaveContent)
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,2),
            TransactionDisposition.VoteCommit,
            Nil)
            
    futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 2), response)) }
	}
  
  test("Vote abort when missing local updates") {
    val store = new NullDataStore(DataStoreID(poolUUID,0)) 
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
    
    val tx = Transaction(crl, messenger, store, txd, LackContent)
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,2),
            TransactionDisposition.VoteAbort,
            List(UpdateErrorResponse(UpdateType.Data, 0, UpdateError.MissingUpdateData, None, None, None)))
            
    futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 2), response)) }
	}
  
  test("Vote abort on revision mismatch") {
    val store = new NullDataStore(DataStoreID(poolUUID,0)) {
      import NullDataStore._
      
      override def getCurrentObjectState(txd: TransactionDescription): Future[ Map[UUID, Either[ObjectError.Value, CurrentObjectState]] ] = {
        var m = Map[UUID, Either[ObjectError.Value, CurrentObjectState]]()
        txd.dataUpdates.foreach(du => m += (du.objectPointer.uuid -> Right(CurrentObjectState(du.objectPointer.uuid, ObjectRevision(9,100), refcount))))
        txd.dataUpdates.foreach(ru => m += (ru.objectPointer.uuid -> Right(CurrentObjectState(ru.objectPointer.uuid, ObjectRevision(9,100), refcount))))
        Future.successful(m)
      }
    }
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
    
    val tx = Transaction(crl, messenger, store, txd, HaveContent)
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,2),
            TransactionDisposition.VoteAbort,
            List(UpdateErrorResponse(UpdateType.Data, 0, UpdateError.RevisionMismatch, Some(ObjectRevision(9,100)), None, None)))
            
    futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 2), response)) }
	}
  
  test("Vote abort on refcount mismatch") {
    val store = new NullDataStore(DataStoreID(poolUUID,0)) {
      import NullDataStore._
      
      override def getCurrentObjectState(txd: TransactionDescription): Future[ Map[UUID, Either[ObjectError.Value, CurrentObjectState]] ] = {
        var m = Map[UUID, Either[ObjectError.Value, CurrentObjectState]]()
        txd.dataUpdates.foreach(du => m += (du.objectPointer.uuid -> Right(CurrentObjectState(du.objectPointer.uuid, revision, ObjectRefcount(9,9)))))
        txd.dataUpdates.foreach(ru => m += (ru.objectPointer.uuid -> Right(CurrentObjectState(ru.objectPointer.uuid, revision, ObjectRefcount(9,9)))))
        Future.successful(m)
      }
    }
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
    
    val tx = Transaction(crl, messenger, store, txd, HaveContent)
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,2),
            TransactionDisposition.VoteAbort,
            List(UpdateErrorResponse(UpdateType.Refcount, 0, UpdateError.RefcountMismatch, None, Some(ObjectRefcount(9,9)), None)))
            
    futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 2), response)) }
	}
  
  test("Vote abort on revision & refcount mismatch") {
    val store = new NullDataStore(DataStoreID(poolUUID,0)) {
      import NullDataStore._
      
      override def getCurrentObjectState(txd: TransactionDescription): Future[ Map[UUID, Either[ObjectError.Value, CurrentObjectState]] ] = {
        var m = Map[UUID, Either[ObjectError.Value, CurrentObjectState]]()
        txd.dataUpdates.foreach(du => m += (du.objectPointer.uuid -> Right(CurrentObjectState(du.objectPointer.uuid, ObjectRevision(9,100), ObjectRefcount(9,9)))))
        txd.dataUpdates.foreach(ru => m += (ru.objectPointer.uuid -> Right(CurrentObjectState(ru.objectPointer.uuid, ObjectRevision(9,100), ObjectRefcount(9,9)))))
        Future.successful(m)
      }
    }
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
    
    val tx = Transaction(crl, messenger, store, txd, HaveContent)
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,2),
            TransactionDisposition.VoteAbort,
            List(
                UpdateErrorResponse(UpdateType.Data, 0, UpdateError.RevisionMismatch, Some(ObjectRevision(9,100)), None, None),
                UpdateErrorResponse(UpdateType.Refcount, 0, UpdateError.RefcountMismatch, None, Some(ObjectRefcount(9,9)), None)))
            
    futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 2), response)) }
	}
  
  test("Vote abort on collision") {
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val collidingTxd = mktxd(Nil, Nil)
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
        
    val store = new NullDataStore(DataStoreID(poolUUID,0)) {
      import NullDataStore._
      
      override def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, TransactionDescription]] = Some(
          Map[UUID, TransactionDescription]((op.uuid -> collidingTxd)))
    }
    
    val tx = Transaction(crl, messenger, store, txd, HaveContent)
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,2),
            TransactionDisposition.VoteAbort,
            List(
                UpdateErrorResponse(UpdateType.Data, 0, UpdateError.Collision, None, None, Some(collidingTxd)),
                UpdateErrorResponse(UpdateType.Refcount, 0, UpdateError.Collision, None, None, Some(collidingTxd))))
            
    futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 2), response)) }
	}
  
  test("Vote abort on collision then vote commit after collision clears") {
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val collidingTxd = mktxd(Nil, Nil)
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
        
    val store = new NullDataStore(DataStoreID(poolUUID,0)) {
      import NullDataStore._
      var ncalls = 0
      override def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, TransactionDescription]] = if (ncalls == 0){
        ncalls += 1
        Some(Map[UUID, TransactionDescription]((op.uuid -> collidingTxd)))
      }
      else
        None
    }
    
    val tx = Transaction(crl, messenger, store, txd, HaveContent)
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,2),
            TransactionDisposition.VoteAbort,
            List(
                UpdateErrorResponse(UpdateType.Data, 0, UpdateError.Collision, None, None, Some(collidingTxd)),
                UpdateErrorResponse(UpdateType.Refcount, 0, UpdateError.Collision, None, None, Some(collidingTxd))))
            
    futureResponse map { 
      
      msg => msg should be ((DataStoreID(poolUUID, 2), response)) 
      
    } flatMap { _ => 
      val futureResponse = messenger.futureMessage
    
      tx.receivePrepare(mkprep(2, 2, txd))
      
      val response = TxPrepareResponse(
              store.storeId, 
              txd.transactionUUID, 
              Right(TxPrepareResponse.Promise(None)), 
              ProposalID(2,2),
              TransactionDisposition.VoteCommit,
              Nil)
              
      futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 2), response)) }
    }
	}
}