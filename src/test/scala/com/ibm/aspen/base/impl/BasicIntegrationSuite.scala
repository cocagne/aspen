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
import com.ibm.aspen.core.transaction.TransactionDriver
import com.ibm.aspen.base.kvtree.KVTreeSimpleFactory
import com.ibm.aspen.base.kvtree.KVTree
import com.ibm.aspen.core.data_store.DataStore
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.read.TriggeredReread
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.base.TestSystem
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.data_store.DataStoreFrontend
import com.ibm.aspen.core.data_store.RocksDBDataStoreBackend
import com.ibm.aspen.core.data_store.MemoryOnlyDataStoreBackend
import com.ibm.aspen.core.crl.MemoryOnlyCRL
import com.ibm.aspen.core.crl.CrashRecoveryLog

object BasicIntegrationSuite {
  trait Closeable {
    def close(): Future[Unit]
  }
}

class BasicIntegrationSuite  extends TempDirSuiteBase {
  import BasicIntegrationSuite._
  import Bootstrap._
  
  override implicit val executionContext = ExecutionContext.Implicits.global
  
  var closeables: List[Closeable] = Nil
  
  override def preTempDirDeletion(): Unit =  {
    val r = Await.result(Future.sequence(closeables.map(_.close())), 1000 milliseconds)
    
    closeables = Nil
  }
  
  def newStore(storeId: DataStoreID): (DataStore, CrashRecoveryLog) = {
    val dbpath = new File(tdir, s"dbdir_${storeId.poolIndex}").getAbsolutePath
    val crlpath = new File(tdir, s"crldir_${storeId.poolIndex}").getAbsolutePath
    //val crl = new RocksDBCrashRecoveryLog(crlpath)(ExecutionContext.Implicits.global) with Closeable
    //val db = new DataStoreFrontend(storeId, new RocksDBDataStoreBackend(dbpath), Nil, Nil) with Closeable
    val crl = new MemoryOnlyCRL() with Closeable
    val db = new DataStoreFrontend(storeId, new MemoryOnlyDataStoreBackend(), Nil, Nil) with Closeable
    Await.result(db.initialized, Duration(10000, MILLISECONDS))
    closeables = db :: crl :: closeables
    (db, crl)
  }

  val noRetry = new NoRetry
  val bootstrapPoolIDA = new Replication(3,2)
  val systemTreeNodeSize = 2048
  def systemTreeNodeCacheFactory(sys: AspenSystem): KVTreeNodeCache = new KVTreeNodeCache {}
  
  def waitForTransactionsComplete(sn: StorageNode): Future[Unit] = Future {
    
    var count = 0
    while (!sn.allTransactionsComplete && count < 100) {
      count += 1
      Thread.sleep(5) 
    }
        
    if (count > 100)
      throw new Exception("Finalization Actions Timed Out")
  }
  
  def mkStorageNode(
      store: DataStore, 
      crl:CrashRecoveryLog,
      net: TestNetwork,
      reader: TriggeredReread,
      radiclePointer: DataObjectPointer): (BasicAspenSystem, StorageNode) = {
    
    val clientId = ClientID(new UUID(0, store.storeId.poolIndex))
    
    val sys = new BasicAspenSystem(
        chooseDesignatedLeader = (o:ObjectPointer) => 0,
        isStorageNodeOnline = (_:StorageNodeID) => true,
        net = new net.CNet(clientId),
        defaultReadDriverFactory = BaseReadDriver.noErrorRecoveryReadDriver(executionContext) _, //reader.triggeredReadDriver(ExecutionContext.Implicits.global) _,
        defaultTransactionDriverFactory = ClientTransactionDriver.noErrorRecoveryFactory,
        defaultAllocationDriverFactory = BaseAllocationDriver.NoErrorRecoveryAllocationDriver,
        transactionFactory = BaseTransaction.Factory,
        storagePoolFactory = BaseStoragePool.Factory,
        bootstrapPoolIDA = bootstrapPoolIDA,
        systemTreeNodeCacheFactory = systemTreeNodeCacheFactory,
        radiclePointer = radiclePointer,
        initializationRetryStrategy = noRetry
        )
    
    val storageNode = new StorageNode(crl, new net.SNet)
    
    object dsFactory extends DataStore.Factory {

      override def apply(
          storeId: DataStoreID,
          transactionRecoveryStates: List[TransactionRecoveryState],
          allocationRecoveryStates: List[AllocationRecoveryState]): Future[DataStore] = Future.successful(store)
    
    }
    
    Await.result(storageNode.addStore(store.storeId, dsFactory.apply), 5000 milliseconds)
    
    (sys, storageNode)
  }
  
  test("Test Bootstrapping Logic") {
    
    val (store0, crl0) = newStore(DataStoreID(BootstrapStoragePoolUUID, 0))
    val (store1, crl1) = newStore(DataStoreID(BootstrapStoragePoolUUID, 1))
    val (store2, crl2) = newStore(DataStoreID(BootstrapStoragePoolUUID, 2))
    
    val radiclePointer = Await.result(Bootstrap.initializeNewSystem(List(store0, store1, store2), bootstrapPoolIDA), 500 milliseconds)
    
    val net = new TestNetwork
    
    val reader = new TriggeredReread
    
    val (sys0, sn0) = mkStorageNode(store0, crl0, net, reader, radiclePointer)
    val (sys1, sn1) = mkStorageNode(store1, crl1, net, reader, radiclePointer)
    val (sys2, sn2) = mkStorageNode(store2, crl2, net, reader, radiclePointer)
    
    Await.result(sys0.radicle, 1000 milliseconds)
    Await.result(sys1.radicle, 1000 milliseconds)
    Await.result(sys2.radicle, 1000 milliseconds)
    
    def recover(sys: BasicAspenSystem, sn: StorageNode): Unit = {
      val kvTreeFactory = new KVTreeSimpleFactory(
          system = sys, 
          treeAllocationPolicyUUID = SystemAllocationPolicyUUID, 
          storagePoolUUID = BootstrapStoragePoolUUID, 
          nodeIDA = bootstrapPoolIDA,
          nodeSize = systemTreeNodeSize, 
          nodeCache = systemTreeNodeCacheFactory(sys), 
          keyComparisonStrategy = KVTree.KeyComparison.Raw)
      
      val faRegistry = BaseFinalizationActionHandlerRegistry(noRetry, sys, kvTreeFactory)
      
      val finalizerFactory = new BaseTransactionFinalizer(sys, faRegistry)
       
      val txMgr = new StorageNodeTransactionManager(sn.crl, sn.net.transactionHandler, TransactionDriver.noErrorRecoveryFactory, finalizerFactory.factory)
      val allocMgr = new StorageNodeAllocationManager(sn.crl, sn.net.allocationHandler)
      
      sn.recoverPendingOperations(txMgr, allocMgr)
    }
    
    recover(sys0, sn0)
    recover(sys1, sn1)
    recover(sys2, sn2)
    
    val sp = Await.result(sys0.getStoragePool(Bootstrap.BootstrapStoragePoolUUID), 100 milliseconds)
    val spAllocTreeDef = Await.result(sp.getAllocationTreeDefinitionPointer(noRetry), 100 milliseconds)
    
    val kvTreeFactory = new KVTreeSimpleFactory(
          system = sys0, 
          treeAllocationPolicyUUID = SystemAllocationPolicyUUID, 
          storagePoolUUID = BootstrapStoragePoolUUID, 
          nodeIDA = bootstrapPoolIDA,
          nodeSize = systemTreeNodeSize, 
          nodeCache = systemTreeNodeCacheFactory(sys0), 
          keyComparisonStrategy = KVTree.KeyComparison.Raw)
    
    val atree = Await.result(kvTreeFactory.createTree(spAllocTreeDef), 100 milliseconds)
    
    var allocTreeEntryCount = 0
    
    def visitEntry(key: Array[Byte], value: Array[Byte]): Unit = synchronized {
      allocTreeEntryCount += 1
    }
    
    Await.result(atree.visitRange(new Array[Byte](0), None, visitEntry), 100 milliseconds)
    
    allocTreeEntryCount should be (BootstrapAllocatedObjectCount)
    
  }
  
  test("Test Allocation & Finalization") {
    
    val (store0, crl0) = newStore(DataStoreID(BootstrapStoragePoolUUID, 0))
    val (store1, crl1) = newStore(DataStoreID(BootstrapStoragePoolUUID, 1))
    val (store2, crl2) = newStore(DataStoreID(BootstrapStoragePoolUUID, 2))
    
    val radiclePointer = Await.result(Bootstrap.initializeNewSystem(List(store0, store1, store2), bootstrapPoolIDA), 500 milliseconds)
    
    val net = new TestNetwork
    
    val reader = new TriggeredReread
    
    val (sys0, sn0) = mkStorageNode(store0, crl0, net, reader, radiclePointer)
    val (sys1, sn1) = mkStorageNode(store1, crl1, net, reader, radiclePointer)
    val (sys2, sn2) = mkStorageNode(store2, crl2, net, reader, radiclePointer)
    
    Await.result(sys0.radicle, 1000 milliseconds)
    Await.result(sys1.radicle, 1000 milliseconds)
    Await.result(sys2.radicle, 1000 milliseconds)
    
    def recover(sys: BasicAspenSystem, sn: StorageNode): Unit = {
      val kvTreeFactory = new KVTreeSimpleFactory(
          system = sys, 
          treeAllocationPolicyUUID = SystemAllocationPolicyUUID, 
          storagePoolUUID = BootstrapStoragePoolUUID, 
          nodeIDA = bootstrapPoolIDA,
          nodeSize = systemTreeNodeSize, 
          nodeCache = systemTreeNodeCacheFactory(sys), 
          keyComparisonStrategy = KVTree.KeyComparison.Raw)
      
      val faRegistry = BaseFinalizationActionHandlerRegistry(noRetry, sys, kvTreeFactory)
      
      val finalizerFactory = new BaseTransactionFinalizer(sys, faRegistry)
       
      val txMgr = new StorageNodeTransactionManager(sn.crl, sn.net.transactionHandler, TransactionDriver.noErrorRecoveryFactory, finalizerFactory.factory)
      val allocMgr = new StorageNodeAllocationManager(sn.crl, sn.net.allocationHandler)
      
      sn.recoverPendingOperations(txMgr, allocMgr)
    }
    
    recover(sys0, sn0)
    recover(sys1, sn1)
    recover(sys2, sn2)
    
    var allocCount = 0
    
    def allocObj(r: Radicle): Future[ObjectPointer] = {
      implicit val tx = sys0.newTransaction()
      val d = DataBuffer(ByteBuffer.allocate(5))
      val ffp = sys0.lowLevelAllocateDataObject(r.systemTreeDefinitionPointer, ObjectRevision.Null, BootstrapStoragePoolUUID,
                                    None, bootstrapPoolIDA, d)
      allocCount += 1
      
      for {
        fp <- ffp
        // Need to give the transaction something to do. Modify refcount instead of data so we don't accidentally corrupt anything
        y = tx.setRefcount(r.systemTreeDefinitionPointer, ObjectRefcount(0,allocCount), ObjectRefcount(0,allocCount + 1))
        committed <- tx.commit()
      } yield {
        fp 
      }
    }
    
    val kvTreeFactory = new KVTreeSimpleFactory(
          system = sys0, 
          treeAllocationPolicyUUID = SystemAllocationPolicyUUID, 
          storagePoolUUID = BootstrapStoragePoolUUID, 
          nodeIDA = bootstrapPoolIDA,
          nodeSize = systemTreeNodeSize, 
          nodeCache = systemTreeNodeCacheFactory(sys0), 
          keyComparisonStrategy = KVTree.KeyComparison.Raw)
    
    var allocTreeEntryCount = 0
    
    def visitEntry(key: Array[Byte], value: Array[Byte]): Unit = synchronized {
      allocTreeEntryCount += 1
    }
    
    for {
      r <- sys0.radicle
      fp1 <- allocObj(r)
      faComplete1 <- waitForTransactionsComplete(sn0)
      fp2 <- allocObj(r)
      faComplete2 <- waitForTransactionsComplete(sn0)
      sp <- sys0.getStoragePool(Bootstrap.BootstrapStoragePoolUUID)
      spAllocTreeDef <- sp.getAllocationTreeDefinitionPointer(noRetry)
      atree <- kvTreeFactory.createTree(spAllocTreeDef)
      visitComplete <- atree.visitRange(new Array[Byte](0), None, visitEntry)
    } yield {
      allocTreeEntryCount should be (BootstrapAllocatedObjectCount + 2)
    }
  }
  
  
  test("Test TestSystem Class") {
    
    val ts = new TestSystem
    
    val sys = ts.aspenSystem
    
    var allocCount = 0
    
    def allocObj(r: Radicle): Future[ObjectPointer] = {
      implicit val tx = sys.newTransaction()
      val d = DataBuffer(ByteBuffer.allocate(5))
      val ffp = sys.lowLevelAllocateDataObject(r.systemTreeDefinitionPointer, ObjectRevision.Null, BootstrapStoragePoolUUID,
                                    None, bootstrapPoolIDA, d)
      allocCount += 1
      
      for {
        fp <- ffp
        // Need to give the transaction something to do. Modify refcount instead of data so we don't accidentally corrupt anything
        y = tx.setRefcount(r.systemTreeDefinitionPointer, ObjectRefcount(0,allocCount), ObjectRefcount(0,allocCount + 1))
        committed <- tx.commit()
      } yield {
        fp 
      }
    }
    
    val kvTreeFactory = new KVTreeSimpleFactory(
          system = sys, 
          treeAllocationPolicyUUID = SystemAllocationPolicyUUID, 
          storagePoolUUID = BootstrapStoragePoolUUID, 
          nodeIDA = bootstrapPoolIDA,
          nodeSize = systemTreeNodeSize, 
          nodeCache = systemTreeNodeCacheFactory(sys), 
          keyComparisonStrategy = KVTree.KeyComparison.Raw)
    
    var allocTreeEntryCount = 0
    
    def visitEntry(key: Array[Byte], value: Array[Byte]): Unit = synchronized {
      allocTreeEntryCount += 1
    }
    
    for {
      r <- sys.radicle
      fp1 <- allocObj(r)
      faComplete1 <- ts.waitForTransactionsComplete(ts.sn0)
      fp2 <- allocObj(r)
      faComplete2 <- ts.waitForTransactionsComplete(ts.sn0)
      sp <- sys.getStoragePool(Bootstrap.BootstrapStoragePoolUUID)
      spAllocTreeDef <- sp.getAllocationTreeDefinitionPointer(noRetry)
      atree <- kvTreeFactory.createTree(spAllocTreeDef)
      visitComplete <- atree.visitRange(new Array[Byte](0), None, visitEntry)
    } yield {
      allocTreeEntryCount should be (BootstrapAllocatedObjectCount + 2)
    }
  }
}