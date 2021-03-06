package com.ibm.aspen.core.transaction

import org.scalatest._
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.ProposalID
import com.ibm.aspen.core.network.NullMessenger
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import java.util.UUID

import com.ibm.aspen.core.objects.ObjectRevision
import java.nio.ByteBuffer

import com.ibm.aspen.base.impl.TransactionStatusCache

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.DataObjectPointer


object TransactionDriverSuite {
  val poolUUID = java.util.UUID.randomUUID()
  val ds0 = DataStoreID(poolUUID, 0)
  val ds1 = DataStoreID(poolUUID, 1)
  val ds2 = DataStoreID(poolUUID, 2)
  val ds3 = DataStoreID(poolUUID, 3)
  
  val rev = ObjectRevision.Null
  val arr = new Array[Byte](0)
  val simpleObj = DataObjectPointer(java.util.UUID.randomUUID(), poolUUID, None, Replication(3,2), 
                                Array(StorePointer(0,arr), StorePointer(1,arr), StorePointer(2,arr)))
  
  //def mkobj = ObjectPointer(java.util.UUID.randomUUID(), poolUUID, None, Replication(3,2), new Array[StorePointer](0))
  
  def mktxd(optr: ObjectPointer, du: List[DataUpdate] = Nil, ru: List[RefcountUpdate] = Nil) = TransactionDescription(
      java.util.UUID.randomUUID(), 100, optr, 0, du ++ ru, Nil)
      
  def mkprep(paxosRound: Int, toPeer: Byte, fromPeer: Byte, txd: TransactionDescription) = TxPrepare(DataStoreID(poolUUID,toPeer), DataStoreID(poolUUID,fromPeer), txd, ProposalID(paxosRound,fromPeer))
  
  class TMessenger extends NullMessenger {
    var messages = List[Message]()
    
    override def send(message: Message): Unit = messages = message :: messages
    
    override def sendPrepare(message: TxPrepare, transactionData: Option[TransactionData] = None): Unit = messages = message :: messages
    
    def clear(): Unit = messages = List()
  }
  
  class TTD (storeId: DataStoreID,
    messenger: StoreSideTransactionMessenger, 
    initialPrepare: TxPrepare, 
    finalizerFactory: TransactionFinalizer.Factory,
    onComplete: (UUID) => Unit) extends TransactionDriver(storeId, messenger, initialPrepare.txd, finalizerFactory) {
    override protected def onFinalized(committed: Boolean): Unit = {
      super.onFinalized(committed)
      onComplete(txd.transactionUUID)
    }
  }
  
  class TFinalizer(autoComplete: Boolean = true) extends TransactionFinalizer with TransactionFinalizer.Factory {
    var cancelled = false
    var created = false
    var peers = Set[DataStoreID]()

    override def cancel(): Unit = cancelled = true
    
    def complete: Future[Unit] = if (autoComplete) Future.successful(()) else Promise[Unit]().future

    def debugStatus: List[(String, Boolean)] = Nil
    
    override def create(txd: TransactionDescription, messenger: StoreSideTransactionMessenger): TransactionFinalizer = {
      created = true
      this
    }

    def updateCommitErrors(commitErrors: Map[DataStoreID, List[UUID]]): Unit = synchronized {
      peers = commitErrors.keySet
    }
  }
  
}

class TransactionDriverSuite extends FunSuite with Matchers {
  import TransactionDriverSuite._

  object noTxCache extends TransactionStatusCache {
    override def transactionAborted(txuuid: UUID): Unit = None

    override def transactionCommitted(txuuid: UUID): Unit = None

    override def transactionFinalized(txuuid: UUID): Unit = None
  }


  test("Simple PrepareResponse Handling") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ds0,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ds1,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages.toSet should be (Set(
        TxAccept(ds0,ds0,txd.transactionUUID,ProposalID(1,0),true),
        TxAccept(ds1,ds0,txd.transactionUUID,ProposalID(1,0),true),
        TxAccept(ds2,ds0,txd.transactionUUID,ProposalID(1,0),true)))
  }

  test("Ignore invalid acceptors") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ds0,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ds3, // invalid, poolIndex doesn't host a slice
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            DataStoreID(java.util.UUID.randomUUID(), 1), // invalid, poolUUID doesn't match
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ds1,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages.toSet should be (Set(
        TxAccept(ds0,ds0,txd.transactionUUID,ProposalID(1,0),true),
        TxAccept(ds1,ds0,txd.transactionUUID,ProposalID(1,0),true),
        TxAccept(ds2,ds0,txd.transactionUUID,ProposalID(1,0),true)))
  }

  test("Multi-object PrepareResponse Handling") {
    val otherPool = java.util.UUID.randomUUID()
    val otherObj = DataObjectPointer(java.util.UUID.randomUUID(), otherPool, None, Replication(3,2),
                                Array(StorePointer(0,arr), StorePointer(1,arr), StorePointer(2,arr)))

    val ods0 = DataStoreID(otherPool, 0)
    val ods1 = DataStoreID(otherPool, 1)

    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: DataUpdate(otherObj, rev, DataUpdateOperation.Overwrite) ::Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ds0,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ds1,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ods0,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ods1,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages.toSet should be (Set(
        TxAccept(ds0,ds0,txd.transactionUUID,ProposalID(1,0),true),
        TxAccept(ds1,ds0,txd.transactionUUID,ProposalID(1,0),true),
        TxAccept(ds2,ds0,txd.transactionUUID,ProposalID(1,0),true)))
  }

  test("Multi-object PrepareResponse Handling - Abort") {
    val otherPool = java.util.UUID.randomUUID()
    val otherObj = DataObjectPointer(java.util.UUID.randomUUID(), otherPool, None, Replication(3,2),
                                Array(StorePointer(0,arr), StorePointer(1,arr), StorePointer(2,arr)))

    val ods0 = DataStoreID(otherPool, 0)
    val ods1 = DataStoreID(otherPool, 1)
    val ods2 = DataStoreID(otherPool, 2)

    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: DataUpdate(otherObj, rev, DataUpdateOperation.Overwrite) ::Nil)
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false

    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ds0,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ds1,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ds2,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ods0,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ods1,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteAbort,
            Nil), noTxCache)

    messenger.messages should be (Nil)

    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0,
            ods2,
            txd.transactionUUID,
            Right(TxPrepareResponse.Promise(None)),
            ProposalID(1,0),
            TransactionDisposition.VoteAbort,
            Nil), noTxCache)
    
    messenger.messages.toSet should be (Set(
        TxAccept(ds0,ds0,txd.transactionUUID,ProposalID(1,0),false),
        TxAccept(ds1,ds0,txd.transactionUUID,ProposalID(1,0),false),
        TxAccept(ds2,ds0,txd.transactionUUID,ProposalID(1,0),false)))     
  }
  
  test("Simple AcceptResponse Handling - Abort") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil) 
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false
    
    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds0,
            ds0, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false)))) 
            
    driver.mayBeDiscarded should be (false)
    completed should be (false)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds0,
            ds1, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false))))
            
    driver.mayBeDiscarded should be (true)
    completed should be (true)
  }
  
  test("Simple AcceptResponse Handling - Commit") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil) 
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer(false)
    val messenger = new TMessenger()
    var completed = false
    
    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds0,
            ds0, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(true)))) 
            
    driver.mayBeDiscarded should be (false)
    completed should be (false)
    finalizer.created should be (false)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds0,
            ds1, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(true))))
            
    driver.mayBeDiscarded should be (false)
    completed should be (false)
    finalizer.created should be (true)
    finalizer.cancelled should be (false)
    
    driver.receiveTxFinalized(TxFinalized(ds0, ds0, txd.transactionUUID, true))
    
    driver.mayBeDiscarded should be (true)
    completed should be (true)
    finalizer.cancelled should be (true)
  }
  
  test("Simple AcceptResponse Handling - Ignore invalid acceptor") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil) 
    val prep = mkprep(1, 0, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false
    
    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds0,
            ds0, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false)))) 
            
    driver.mayBeDiscarded should be (false)
    completed should be (false)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds0,
            ds3, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false)))) 
            
    driver.mayBeDiscarded should be (false)
    completed should be (false)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds0,
            ds1, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false))))
            
    driver.mayBeDiscarded should be (true)
    completed should be (true)
  }
}