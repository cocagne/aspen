package com.ibm.aspen.core.transaction

import org.scalatest._
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.paxos.ProposalID
import com.ibm.aspen.core.network.NullMessenger
import com.ibm.aspen.core.network.TransactionMessenger
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision

object TransactionDriverSuite {
  val poolUUID = java.util.UUID.randomUUID()
  val ds0 = DataStoreID(poolUUID, 0)
  val ds1 = DataStoreID(poolUUID, 1)
  val ds2 = DataStoreID(poolUUID, 2)
  val ds3 = DataStoreID(poolUUID, 3)
  
  val rev = ObjectRevision(0,1)
  val arr = new Array[Byte](0)
  val simpleObj = ObjectPointer(java.util.UUID.randomUUID(), poolUUID, None, Replication(3,2), 
                                Array(StorePointer(0,arr), StorePointer(1,arr), StorePointer(2,arr)))
  
  //def mkobj = ObjectPointer(java.util.UUID.randomUUID(), poolUUID, None, Replication(3,2), new Array[StorePointer](0))
  
  def mktxd(optr: ObjectPointer, du: List[DataUpdate] = Nil, ru: List[RefcountUpdate] = Nil) = TransactionDescription(
      java.util.UUID.randomUUID(), 100, optr, 0, du, ru, Nil)
      
  def mkprep(paxosRound: Int, fromPeer: Byte, txd: TransactionDescription) = TxPrepare(DataStoreID(poolUUID,fromPeer), txd, ProposalID(paxosRound,fromPeer))
  
  class TMessenger extends NullMessenger {
    var messages = List[(DataStoreID,Message)]()
    
    override def send(toStore: DataStoreID, message: Message, updateContent: Option[LocalUpdateContent]): Unit = messages = (toStore,message) :: messages
    
    def clear(): Unit = messages = List()
  }
  
  class TTD (storeId: DataStoreID,
    messenger:TransactionMessenger, 
    initialPrepare: TxPrepare, 
    finalizerFactory: TransactionFinalizer.Factory,
    onComplete: (UUID) => Unit) extends TransactionDriver(storeId, messenger, initialPrepare, finalizerFactory, onComplete)
  
  class TFinalizer extends TransactionFinalizer with TransactionFinalizer.Factory {
    var cancelled = false
    var created = false
    var peers = Set[DataStoreID]()
    
    override def cancel(): Unit = cancelled = true
    
    override def create(txd: TransactionDescription, acceptedPeers: Set[DataStoreID], messenger: TransactionMessenger): TransactionFinalizer = {
      created = true
      peers = acceptedPeers
      this
    }
  }
  
}

class TransactionDriverSuite extends FunSuite with Matchers {
  import TransactionDriverSuite._
  
  test("Simple PrepareResponse Handling") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil) 
    val prep = mkprep(1, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false
    
    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    messenger.messages should be (Nil)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds1, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    val acc = TxAccept(ds0,txd.transactionUUID,ProposalID(1,0),true)
    
    messenger.messages.toSet should be (Set((ds0,acc), (ds1,acc), (ds2,acc)))     
  }
  
  test("Ignore invalid acceptors") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil) 
    val prep = mkprep(1, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false
    
    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    messenger.messages should be (Nil)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds3, // invalid, poolIndex doesn't host a slice
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    messenger.messages should be (Nil)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            DataStoreID(java.util.UUID.randomUUID(), 1), // invalid, poolUUID doesn't match
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    messenger.messages should be (Nil)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds1, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    val acc = TxAccept(ds0,txd.transactionUUID,ProposalID(1,0),true)
    
    messenger.messages.toSet should be (Set((ds0,acc), (ds1,acc), (ds2,acc)))     
  }
  
  test("Multi-object PrepareResponse Handling") {
    val otherPool = java.util.UUID.randomUUID()
    val otherObj = ObjectPointer(java.util.UUID.randomUUID(), otherPool, None, Replication(3,2), 
                                Array(StorePointer(0,arr), StorePointer(1,arr), StorePointer(2,arr)))
                                
    val ods0 = DataStoreID(otherPool, 0)
    val ods1 = DataStoreID(otherPool, 1)
    
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: DataUpdate(otherObj, rev, DataUpdateOperation.Overwrite) ::Nil) 
    val prep = mkprep(1, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false
    
    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    messenger.messages should be (Nil)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds1, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    messenger.messages should be (Nil)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ods0, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    messenger.messages should be (Nil)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ods1, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    val acc = TxAccept(ds0,txd.transactionUUID,ProposalID(1,0),true)
    
    messenger.messages.toSet should be (Set((ds0,acc), (ds1,acc), (ds2,acc)))     
  }
  
  test("Multi-object PrepareResponse Handling - Abort") {
    val otherPool = java.util.UUID.randomUUID()
    val otherObj = ObjectPointer(java.util.UUID.randomUUID(), otherPool, None, Replication(3,2), 
                                Array(StorePointer(0,arr), StorePointer(1,arr), StorePointer(2,arr)))
                                
    val ods0 = DataStoreID(otherPool, 0)
    val ods1 = DataStoreID(otherPool, 1)
    
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: DataUpdate(otherObj, rev, DataUpdateOperation.Overwrite) ::Nil) 
    val prep = mkprep(1, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false
    
    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds0, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    messenger.messages should be (Nil)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ds1, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    messenger.messages should be (Nil)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ods0, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteCommit,
            Nil))
            
    messenger.messages should be (Nil)
    
    driver.receiveTxPrepareResponse(TxPrepareResponse(
            ods1, 
            txd.transactionUUID, 
            Right(TxPrepareResponse.Promise(None)), 
            ProposalID(1,0),
            TransactionDisposition.VoteAbort,
            Nil))
            
    val acc = TxAccept(ds0,txd.transactionUUID,ProposalID(1,0),false)
    
    messenger.messages.toSet should be (Set((ds0,acc), (ds1,acc), (ds2,acc)))     
  }
  
  test("Simple AcceptResponse Handling - Abort") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil) 
    val prep = mkprep(1, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false
    
    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds0, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false)))) 
            
    driver.mayBeDiscarded should be (false)
    completed should be (false)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds1, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false))))
            
    driver.mayBeDiscarded should be (true)
    completed should be (true)
  }
  
  test("Simple AcceptResponse Handling - Commit") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil) 
    val prep = mkprep(1, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false
    
    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds0, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(true)))) 
            
    driver.mayBeDiscarded should be (false)
    completed should be (false)
    finalizer.created should be (false)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds1, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(true))))
            
    driver.mayBeDiscarded should be (false)
    completed should be (false)
    finalizer.created should be (true)
    finalizer.cancelled should be (false)
    
    driver.receiveTxFinalized(TxFinalized(ds0, txd.transactionUUID, true))
    
    driver.mayBeDiscarded should be (true)
    completed should be (true)
    finalizer.cancelled should be (true)
    finalizer.peers should be (Set(ds0, ds1))
  }
  
  test("Simple AcceptResponse Handling - Ignore invalid acceptor") {
    val txd = mktxd(simpleObj, DataUpdate(simpleObj, rev, DataUpdateOperation.Overwrite) :: Nil) 
    val prep = mkprep(1, 0, txd)
    val finalizer = new TFinalizer()
    val messenger = new TMessenger()
    var completed = false
    
    val driver =  new TTD(ds0, messenger, prep, finalizer, uuid => completed = true)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds0, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false)))) 
            
    driver.mayBeDiscarded should be (false)
    completed should be (false)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds3, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false)))) 
            
    driver.mayBeDiscarded should be (false)
    completed should be (false)
    
    driver.receiveTxAcceptResponse(TxAcceptResponse(
            ds1, 
            txd.transactionUUID, 
            ProposalID(1,0),
            Right(TxAcceptResponse.Accepted(false))))
            
    driver.mayBeDiscarded should be (true)
    completed should be (true)
  }
}