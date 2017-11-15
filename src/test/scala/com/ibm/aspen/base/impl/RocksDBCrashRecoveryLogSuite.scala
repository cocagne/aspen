package com.ibm.aspen.base.impl

import com.ibm.aspen.core.transaction.TransactionSuite
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TransactionDisposition
import com.ibm.aspen.core.transaction.TransactionStatus
import com.ibm.aspen.core.transaction.paxos.PersistentState
import com.ibm.aspen.core.transaction.paxos.ProposalID
import scala.concurrent.Await
import scala.concurrent.duration._
import java.io.File
import scala.concurrent.ExecutionContext
import java.nio.ByteBuffer
import java.util.UUID
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.DataBuffer

object RocksDBCrashRecoveryLogSuite {
  import TransactionSuite._
  
  val storeId = DataStoreID(poolUUID, 1)
  
  val awaitDuration = Duration(100, MILLISECONDS)
}

class RocksDBCrashRecoveryLogSuite extends TempDirSuiteBase {
  
  import TransactionSuite._
  import RocksDBCrashRecoveryLogSuite._
  
  var crl: RocksDBCrashRecoveryLog = null
  
  override def preTempDirDeletion(): Unit = if (crl!=null) crl.immediateClose()
  
  def newCRL() = {
    val tpath = new File(tdir, "dbdir").getAbsolutePath
    
    crl = new RocksDBCrashRecoveryLog(tpath)(ExecutionContext.Implicits.global)
  }
  
  test("Save without data") {
    val txd = mktxd(Nil, Nil)
    val promisedId = ProposalID(5,1)
    
    val trs = TransactionRecoveryState(
        storeId, txd, LackContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(Some(promisedId), None))
    
    newCRL()
    
    Await.result(crl.saveTransactionRecoveryState(trs), awaitDuration)
    
    crl.immediateClose()
    
    newCRL()
    
    val lst = Await.result(crl.initialize(), awaitDuration)
    
    lst should be ( trs :: Nil )
  }
  
  test("Update Entry") {
    val txd = mktxd(Nil, Nil)
    val promisedId = ProposalID(5,1)
    val promisedId2 = ProposalID(6,2)
    
    val trs = TransactionRecoveryState(
        storeId, txd, LackContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(Some(promisedId), None))
    
    newCRL()
    
    Await.result(crl.saveTransactionRecoveryState(trs), awaitDuration)
    
    val trs2 = TransactionRecoveryState(
        storeId, txd, LackContent, TransactionDisposition.VoteCommit, TransactionStatus.Committed, PersistentState(Some(promisedId2), Some((promisedId2,true))))
    
    Await.result(crl.saveTransactionRecoveryState(trs2), awaitDuration)
    
    crl.immediateClose()
    
    newCRL()
    
    val lst = Await.result(crl.initialize(), awaitDuration)
    
    lst should be ( trs2 :: Nil )
  }
  
  test("Save with data") {
    val txd = mktxd(Nil, Nil)
    val promisedId = ProposalID(5,1)
    
    val d1 = List[Byte](1,2,3).toArray
    val d2 = List[Byte](4,5,6).toArray
    val uuid1 = new UUID(1,1)
    val uuid2 = new UUID(2,2)
    val lu1 = LocalUpdate(uuid1, DataBuffer(d1))
    val lu2 = LocalUpdate(uuid2, DataBuffer(d2))
    
    val DataContent = Some(List(lu1, lu2))
    
    val trs = TransactionRecoveryState(
        storeId, txd, DataContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(Some(promisedId), None))
    
    newCRL()
    
    Await.result(crl.saveTransactionRecoveryState(trs), awaitDuration)
    
    crl.immediateClose()
    
    newCRL()
    
    val lst = Await.result(crl.initialize(), awaitDuration)
    
    lst.length should be ( 1 )
    lst.head.copy(localUpdates=None) should be ( trs.copy(localUpdates=None)  )
    val a1 = DataContent.get
    val a2 = lst.head.localUpdates.get
    a2.size should be (a1.size)
    
    def bb2arr(bb:ByteBuffer) = {
      val a = new Array[Byte](bb.capacity)
      bb.asReadOnlyBuffer().get(a)
      a
    }
    
    a1 zip a2 foreach { t => 
      t._1.objectUUID should be (t._2.objectUUID)
      java.util.Arrays.equals(t._1.data.getByteArray(), t._2.data.getByteArray())  
    }
    
    1 should be (1)
  }
  
  test("Delete Entry") {
    val txd = mktxd(Nil, Nil)
    val promisedId = ProposalID(5,1)
    
    val d1 = List[Byte](1,2,3).toArray
    val d2 = List[Byte](4,5,6).toArray
    val uuid1 = new UUID(1,1)
    val uuid2 = new UUID(2,2)
    val lu1 = LocalUpdate(uuid1, DataBuffer(d1))
    val lu2 = LocalUpdate(uuid2, DataBuffer(d2))
    
    val DataContent = Some(List(lu1, lu2))
    
    val trs = TransactionRecoveryState(
        storeId, txd, DataContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(Some(promisedId), None))
    
    newCRL()
    
    Await.result(crl.saveTransactionRecoveryState(trs), awaitDuration)
    
    Await.result(crl.confirmedDiscardTransactionState(txd), awaitDuration)
    
    crl.immediateClose()
    
    newCRL()
    
    val lst = Await.result(crl.initialize(), awaitDuration)
    
    lst should be ( Nil )
  }
}