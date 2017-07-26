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
        storeId, txd, HaveContent, TransactionDisposition.Undetermined, TransactionStatus.Unresolved, PersistentState(Some(promisedId), None))
    
    newCRL()
    
    Await.result(crl.saveTransactionRecoveryState(trs, None), awaitDuration)
    
    crl.immediateClose()
    
    newCRL()
    
    val lst = Await.result(crl.initialize(), awaitDuration)
    
    lst should be ( trs :: Nil )
  }
}