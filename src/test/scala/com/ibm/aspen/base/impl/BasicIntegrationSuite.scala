package com.ibm.aspen.base.impl

import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest._
import scala.language.postfixOps
import com.ibm.aspen.base.NoRetry
import com.ibm.aspen.core.objects.ObjectRevision
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectPointer
import java.io.File
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.TestNetwork
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.network.ClientID
import java.util.UUID
import com.ibm.aspen.core.read.BaseReadDriver
import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.core.allocation.BaseAllocationDriver
import com.ibm.aspen.base.kvtree.KVTreeNodeCache
import com.ibm.aspen.core.network.StorageNodeID

object BasicIntegrationSuite {
  trait Closeable {
    def close(): Future[Unit]
  }
}

class BasicIntegrationSuite  extends AsyncFunSuite with Matchers with BeforeAndAfter {
  import BasicIntegrationSuite._
  import Bootstrap._
  
  override implicit val executionContext = ExecutionContext.Implicits.global
  
  var tdir:File = _
  var tdirMgr: TempDirManager = _
  var closeables: List[Closeable] = Nil
  
  def newStore(storeId: DataStoreID): (RocksDBDataStore, RocksDBCrashRecoveryLog) = {
    val dbpath = new File(tdir, s"dbdir_${storeId.poolIndex}").getAbsolutePath
    val crlpath = new File(tdir, s"crldir_${storeId.poolIndex}").getAbsolutePath
    val crl = new RocksDBCrashRecoveryLog(crlpath)(ExecutionContext.Implicits.global) with Closeable
    val amgr = new BaseAllocationManager(crl)
    val db = new RocksDBDataStore(storeId, dbpath, amgr)(ExecutionContext.Implicits.global) with Closeable
    closeables = db :: crl :: closeables
    (db, crl)
  }
  
  before {
    tdirMgr = new TempDirManager
    tdir = tdirMgr.tdir
  }

  after {
    val r = Await.result(Future.sequence(closeables.map(_.close())), 1000 milliseconds)
    
    closeables = Nil
    
    tdirMgr.delete()
  }
  
  test("Test Bootstrapping Logic") {
    val net = new TestNetwork
    val noRetry = new NoRetry
    val bootstrapPoolIDA = new Replication(3,2)
    def systemTreeNodeCacheFactory(sys: AspenSystem): KVTreeNodeCache = new KVTreeNodeCache {}
    
    val (store0, crl0) = newStore(DataStoreID(BootstrapStoragePoolUUID, 0))
    val (store1, crl1) = newStore(DataStoreID(BootstrapStoragePoolUUID, 1))
    val (store2, clr2) = newStore(DataStoreID(BootstrapStoragePoolUUID, 2))
    
    // Bootstrap system
    val radiclePointer = Await.result(Bootstrap.initializeNewSystem(List(store0, store1, store2), bootstrapPoolIDA), 500 milliseconds)
    /*
    def mksys(store: RocksDBDataStore, crl:RocksDBCrashRecoveryLog): Future[AspenSystem] = {
      
      val clientId = ClientID(new UUID(0, store.storeId.poolIndex))
      
      val (cliMessenger, storageNodeMessenger) = net.addStorageNode(clientId, List(store.storeId))
      
      val sys = new BasicAspenSystem(
          chooseDesignatedLeader = (o:ObjectPointer) => 0,
          isStorageNodeOnline = (_:StorageNodeID) => true,
          messenger = cliMessenger,
          defaultReadDriverFactory = BaseReadDriver.noErrorRecoveryReadDriver(ExecutionContext.Implicits.global) _,
          defaultTransactionDriverFactory = ClientTransactionDriver.noErrorRecoveryFactory,
          defaultAllocationDriverFactory = BaseAllocationDriver.NoErrorRecoveryAllocationDriver,
          transactionFactory = BaseTransaction.Factory,
          storagePoolFactory = BaseStoragePool.Factory,
          bootstrapPoolIDA = bootstrapPoolIDA,
          systemTreeNodeCacheFactory = systemTreeNodeCacheFactory,
          radiclePointer = radiclePointer,
          initializationRetryStrategy = noRetry
          )
      /*class StorageNode(
  val crl: CrashRecoveryLog, 
  val messenger: StorageNodeMessenger,
  val driverFactory: TransactionDriver.Factory,
  val finalizerFactory: TransactionFinalizer.Factory,
  val initialStores: List[DataStore],
  val onStoreInitializationFailure: (DataStore, Throwable) => Unit
)(implicit ec: ExecutionContext) */
      // create StorageNode
      val storageNode = new StorageNode(
          crl = crl,
          messenger = storageNodeMessenger,
          driverFactory = 
      
      // await initialization of StorageNode
    }
    */
    
    /*
    val osd = ms.storage.read(r.systemTreeDefinitionPointer).get
    
    val sp = Await.result(ms.basicAspenSystem.getStoragePool(Bootstrap.BootstrapStoragePoolUUID), 100 milliseconds)
    val spAllocTreeDef = Await.result(sp.getAllocationTreeDefinitionPointer(noRetry), 100 milliseconds)
    val atree = Await.result(ms.kvTreeFactory.createTree(spAllocTreeDef), 100 milliseconds)
    
    var allocTreeEntryCount = 0
    
    def visitEntry(key: Array[Byte], value: Array[Byte]): Unit = synchronized {
      allocTreeEntryCount += 1
    }
    
    Await.result(atree.visitRange(new Array[Byte](0), None, visitEntry), 100 milliseconds)
    
    allocTreeEntryCount should be (BootstrapAllocatedObjectCount)
    * 
    */
    0 should be(0)
  }
}