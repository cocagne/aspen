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
import java.nio.ByteBuffer

object TransactionSuite {
  
  val poolUUID = java.util.UUID.randomUUID()
  
  def mkobj = ObjectPointer(java.util.UUID.randomUUID(), poolUUID, None, Replication(3,2), new Array[StorePointer](0))
  
  def mktxd(du: List[DataUpdate], ru: List[RefcountUpdate]) = TransactionDescription(
      java.util.UUID.randomUUID(), 100, mkobj, 0, du, ru, Nil)
      
  def mkprep(paxosRound: Int, fromPeer: Byte, txd: TransactionDescription) = TxPrepare(DataStoreID(poolUUID,fromPeer), txd, ProposalID(paxosRound,fromPeer))
  
  val HaveContent = Some(List(ByteBuffer.wrap(new Array[Byte](0)), ByteBuffer.wrap(new Array[Byte](0)), ByteBuffer.wrap(new Array[Byte](0))).toArray)
  val LackContent = None
  
  class TMessenger extends NullMessenger {
    var p = Promise[(DataStoreID,Message)]()
    
    def futureMessage = p.future
    
    override def send(toStore: DataStoreID, message: Message, updateContent: Option[Array[ByteBuffer]]): Unit = {
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
    val tx = new Transaction(crl, messenger, t => (), store, TransactionRecoveryState(
        store.storeId, txd, HaveContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(Some(promisedId), None)))
    
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
    
    val tx = Transaction(crl, messenger, t => (), store, txd, HaveContent)
    
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
      override def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = {
        crlStateSaved = true
        super.saveTransactionRecoveryState(state)
      }
    }
    val messenger = new TMessenger {
      override def send(toStore: DataStoreID, message: Message, updateContent: Option[Array[ByteBuffer]]): Unit = {
        stateSavedBeforeMessageSent = crlStateSaved
        super.send(toStore, message)
      }
    }
    
    val txd = mktxd(Nil, Nil)
    
    val tx = Transaction(crl, messenger, t => (), store, txd, HaveContent)
    
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
    
    val tx = Transaction(crl, messenger, t => (), store, txd, HaveContent)
    
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
    
    val tx = Transaction(crl, messenger, t => (), store, txd, LackContent)
    
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
        txd.dataUpdates.foreach(du => m += (du.objectPointer.uuid -> Right(CurrentObjectState(du.objectPointer.uuid, ObjectRevision(9,100), refcount, None))))
        txd.dataUpdates.foreach(ru => m += (ru.objectPointer.uuid -> Right(CurrentObjectState(ru.objectPointer.uuid, ObjectRevision(9,100), refcount, None))))
        Future.successful(m)
      }
    }
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
    
    val tx = Transaction(crl, messenger, t => (), store, txd, HaveContent)
    
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
        txd.dataUpdates.foreach(du => m += (du.objectPointer.uuid -> Right(CurrentObjectState(du.objectPointer.uuid, revision, ObjectRefcount(9,9), None))))
        txd.dataUpdates.foreach(ru => m += (ru.objectPointer.uuid -> Right(CurrentObjectState(ru.objectPointer.uuid, revision, ObjectRefcount(9,9), None))))
        Future.successful(m)
      }
    }
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
    
    val tx = Transaction(crl, messenger, t => (), store, txd, HaveContent)
    
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
        txd.dataUpdates.foreach(du => m += (du.objectPointer.uuid -> Right(CurrentObjectState(du.objectPointer.uuid, ObjectRevision(9,100), ObjectRefcount(9,9), None))))
        txd.dataUpdates.foreach(ru => m += (ru.objectPointer.uuid -> Right(CurrentObjectState(ru.objectPointer.uuid, ObjectRevision(9,100), ObjectRefcount(9,9), None))))
        Future.successful(m)
      }
    }
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
    
    val tx = Transaction(crl, messenger, t => (), store, txd, HaveContent)
    
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
      
      override def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, Either[ObjectError.Value, TransactionDescription]]] = Some(
          Map[UUID, Either[ObjectError.Value, TransactionDescription]]((op.uuid -> Right(collidingTxd))))
    }
    
    val tx = Transaction(crl, messenger, t => (), store, txd, HaveContent)
    
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
  
  test("Vote abort on object error") {
    val messenger = new TMessenger
    val crl = new NullCRL
    val op = mkobj
    val collidingTxd = mktxd(Nil, Nil)
    val txd = mktxd(
        DataUpdate(op, NullDataStore.revision, DataUpdateOperation.Overwrite) :: Nil, 
        RefcountUpdate(op, NullDataStore.refcount, ObjectRefcount(2,150)) :: Nil)
        
    val store = new NullDataStore(DataStoreID(poolUUID,0)) {
      import NullDataStore._
      
      override def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, Either[ObjectError.Value, TransactionDescription]]] = Some(
          Map[UUID, Either[ObjectError.Value, TransactionDescription]]((op.uuid -> Left(ObjectError.InvalidLocalPointer))))
    }
    
    val tx = Transaction(crl, messenger, t => (), store, txd, HaveContent)
    
    val futureResponse = messenger.futureMessage
    
    tx.receivePrepare(mkprep(1, 2, txd))
    
    val response = TxPrepareResponse(
            store.storeId, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,2),
            TransactionDisposition.VoteAbort,
            List(
                UpdateErrorResponse(UpdateType.Data, 0, UpdateError.InvalidLocalPointer, None, None, None),
                UpdateErrorResponse(UpdateType.Refcount, 0, UpdateError.InvalidLocalPointer, None, None, None)))
            
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
      override def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, Either[ObjectError.Value, TransactionDescription]]] = if (ncalls == 0){
        ncalls += 1
        Some(Map[UUID, Either[ObjectError.Value, TransactionDescription]]((op.uuid -> Right(collidingTxd))))
      }
      else
        None
    }
    
    val tx = Transaction(crl, messenger, t => (), store, txd, HaveContent)
    
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
  
  //-----------------------------------------------------------------------------------------------
  // Accept Handling
  //-----------------------------------------------------------------------------------------------
  
  test("Nack accept message") {
    val store = new NullDataStore(DataStoreID(poolUUID,0))
    val messenger = new TMessenger
    val crl = new NullCRL
    val txd = mktxd(Nil, Nil)
    
    val promisedId = ProposalID(5,1)
    val tx = new Transaction(crl, messenger, t => (), store, TransactionRecoveryState(
        store.storeId, txd, HaveContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(Some(promisedId), None)))
    
    val futureResponse = messenger.futureMessage
    
    val pid = ProposalID(1,0)
    
    tx.receiveAccept(TxAccept(DataStoreID(poolUUID,0), txd.transactionUUID, pid, false))
    
    val response = TxAcceptResponse(
            store.storeId, 
            txd.transactionUUID,
            pid,
            Left(TxAcceptResponse.Nack(promisedId)))
            
    futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 0), response)) }
	}
  
  test("Accept accept message") {
    val store = new NullDataStore(DataStoreID(poolUUID,0))
    val messenger = new TMessenger
    val crl = new NullCRL
    val txd = mktxd(Nil, Nil)
    
    val promisedId = ProposalID(5,1)
    val tx = new Transaction(crl, messenger, t => (), store, TransactionRecoveryState(
        store.storeId, txd, HaveContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(Some(promisedId), None)))
    
    val futureResponse = messenger.futureMessage
    
    val pid = ProposalID(6,0)
    
    tx.receiveAccept(TxAccept(DataStoreID(poolUUID,0), txd.transactionUUID, pid, false))
    
    val response = TxAcceptResponse(
            store.storeId, 
            txd.transactionUUID,
            pid,
            Right(TxAcceptResponse.Accepted(false)))
            
    futureResponse map { msg => msg should be ((DataStoreID(poolUUID, 0), response)) }
	}
  
  test("Verify state persisted before accepted response send") {
    var stateSavedBeforeMessageSent = false
    var crlStateSaved = false
    
    val store = new NullDataStore(DataStoreID(poolUUID,0))
    val crl = new NullCRL {
      override def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = {
        crlStateSaved = true
        super.saveTransactionRecoveryState(state)
      }
    }
    val messenger = new TMessenger {
      override def send(toStore: DataStoreID, message: Message, updateContent: Option[Array[ByteBuffer]]): Unit = {
        stateSavedBeforeMessageSent = crlStateSaved
        super.send(toStore, message)
      }
    }
    
    val txd = mktxd(Nil, Nil)
    
    val tx = Transaction(crl, messenger, t => (), store, txd, HaveContent)
    
    val futureResponse = messenger.futureMessage
    
    val pid = ProposalID(6,2)
    
    tx.receiveAccept(TxAccept(DataStoreID(poolUUID,2), txd.transactionUUID, pid, false))
    
    val response = TxAcceptResponse(
            store.storeId, 
            txd.transactionUUID,
            pid,
            Right(TxAcceptResponse.Accepted(false)))
            
    futureResponse map { 
      msg => 
        msg should be ((DataStoreID(poolUUID, 2), response))
        stateSavedBeforeMessageSent should be (true)
    }
	}
  
  test("Receive Accept Response - discard state on transaction abort") {
    var stateDiscarded = false
    
    val store = new NullDataStore(DataStoreID(poolUUID,0)) {
      override def discardTransaction(txd: TransactionDescription): Unit = {
        stateDiscarded = true
      }
    }
    val messenger = new TMessenger
    val crl = new NullCRL
    val txd = mktxd(Nil, Nil)
    
    val tx = new Transaction(crl, messenger, t => (), store, TransactionRecoveryState(
        store.storeId, txd, HaveContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(None, None)))
    
    tx.receiveAcceptResponse(TxAcceptResponse(
            DataStoreID(poolUUID,0), 
            txd.transactionUUID,
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false)))) should be (None)
    
    tx.receiveAcceptResponse(TxAcceptResponse(
            DataStoreID(poolUUID,1), 
            txd.transactionUUID,
            ProposalID(6,0),
            Left(TxAcceptResponse.Nack(ProposalID(2,2))))) should be (None)    
    
    tx.receiveAcceptResponse(TxAcceptResponse(
            DataStoreID(poolUUID,0), 
            txd.transactionUUID,
            ProposalID(3,0),
            Right(TxAcceptResponse.Accepted(false)))) should be (None)
    
    tx.receiveAcceptResponse(TxAcceptResponse(
            DataStoreID(poolUUID,0), 
            txd.transactionUUID,
            ProposalID(3,0),
            Right(TxAcceptResponse.Accepted(false)))) should be (None)
    
    tx.receiveAcceptResponse(TxAcceptResponse(
            DataStoreID(poolUUID,1), 
            txd.transactionUUID,
            ProposalID(3,0),
            Right(TxAcceptResponse.Accepted(false)))) should be (Some(false))
            
    stateDiscarded should be (true)
	}
  
  test("Receive Accept Response - commit state on transaction commit") {
    var stateCommitted = false
    
    val store = new NullDataStore(DataStoreID(poolUUID,0)) {
      override def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[Array[ByteBuffer]]): Future[Unit] = {
        stateCommitted = true
        Future.successful(())
      }
    }
    val messenger = new TMessenger
    val crl = new NullCRL
    val txd = mktxd(Nil, Nil)
    
    val tx = new Transaction(crl, messenger, t => (), store, TransactionRecoveryState(
        store.storeId, txd, HaveContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(None, None)))
    
    tx.receiveAcceptResponse(TxAcceptResponse(
            DataStoreID(poolUUID,0), 
            txd.transactionUUID,
            ProposalID(3,0),
            Right(TxAcceptResponse.Accepted(true)))) should be (None)
    
    tx.receiveAcceptResponse(TxAcceptResponse(
            DataStoreID(poolUUID,1), 
            txd.transactionUUID,
            ProposalID(3,0),
            Right(TxAcceptResponse.Accepted(true)))) should be (Some(true))
            
    stateCommitted should be (true)
	}
  
  test("Receive Accept Response - single call to commit") {
    var calls = 0
    
    val store = new NullDataStore(DataStoreID(poolUUID,0)) {
      override def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[Array[ByteBuffer]]): Future[Unit] = {
        calls += 1
        Future.successful(())
      }
    }
    val messenger = new TMessenger
    val crl = new NullCRL
    val txd = mktxd(Nil, Nil)
    
    val tx = new Transaction(crl, messenger, t => (), store, TransactionRecoveryState(
        store.storeId, txd, HaveContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(None, None)))
    
    tx.receiveAcceptResponse(TxAcceptResponse(
            DataStoreID(poolUUID,0), 
            txd.transactionUUID,
            ProposalID(3,0),
            Right(TxAcceptResponse.Accepted(true)))) should be (None)
    
    tx.receiveAcceptResponse(TxAcceptResponse(
            DataStoreID(poolUUID,1), 
            txd.transactionUUID,
            ProposalID(3,0),
            Right(TxAcceptResponse.Accepted(true)))) should be (Some(true))
            
    calls should be (1)
    
    tx.receiveAcceptResponse(TxAcceptResponse(
            DataStoreID(poolUUID,2), 
            txd.transactionUUID,
            ProposalID(3,0),
            Right(TxAcceptResponse.Accepted(true)))) should be (Some(true))
            
    calls should be (1)
	}
  
  test("Receive Finalized - commit state on finalize if not already committed") {
    var stateCommitted = false
    
    val store = new NullDataStore(DataStoreID(poolUUID,0)) {
      override def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[Array[ByteBuffer]]): Future[Unit] = {
        stateCommitted = true
        Future.successful(())
      }
    }
    val messenger = new TMessenger
    val crl = new NullCRL
    val txd = mktxd(Nil, Nil)
    
    val tx = new Transaction(crl, messenger, t => (), store, TransactionRecoveryState(
        store.storeId, txd, HaveContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(None, None)))
    
    tx.receiveFinalized(TxFinalized(
            DataStoreID(poolUUID,1), 
            txd.transactionUUID,
            true)) 
            
    stateCommitted should be (true)
	}
}