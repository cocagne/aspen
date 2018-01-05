package com.ibm.aspen.base

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.DataStore
import com.ibm.aspen.core.data_store.MemoryOnlyDataStore
import com.ibm.aspen.core.crl.MemoryOnlyCRL
import scala.concurrent.ExecutionContext.Implicits.global
import com.ibm.aspen.base.kvtree.KVTreeNodeCache
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.base.impl.StorageNode
import scala.concurrent.Future
import com.ibm.aspen.core.network.TestNetwork
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.impl.BasicAspenSystem
import com.ibm.aspen.core.network.ClientID
import java.util.UUID
import com.ibm.aspen.core.network.StorageNodeID
import com.ibm.aspen.core.read.BaseReadDriver
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.core.allocation.BaseAllocationDriver
import com.ibm.aspen.base.impl.BaseTransaction
import com.ibm.aspen.base.impl.BaseStoragePool
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import scala.concurrent.Await
import scala.concurrent.duration._
import com.ibm.aspen.base.impl.Bootstrap
import com.ibm.aspen.base.kvtree.KVTreeSimpleFactory
import com.ibm.aspen.base.impl.FinalizationActionRegistry
import com.ibm.aspen.base.impl.BaseTransactionFinalizer
import com.ibm.aspen.base.impl.StorageNodeTransactionManager
import com.ibm.aspen.base.impl.StorageNodeAllocationManager
import com.ibm.aspen.base.kvtree.KVTree
import com.ibm.aspen.core.transaction.TransactionDriver
import com.ibm.aspen.core.data_store.BootstrapDataStore

object TestSystem {
  def memoryStoreFactory(storeId: DataStoreID): (BootstrapDataStore, CrashRecoveryLog) = {
    (new MemoryOnlyDataStore(storeId), new MemoryOnlyCRL)
  }
  
  def noTreeNodeCacheFactory(sys: AspenSystem): KVTreeNodeCache = new KVTreeNodeCache {}
  
  val DefaultIDA = new Replication(3,2)
  
  val DefaultSystemTreeNodeSize = 2048
  
  val NoRetry = new com.ibm.aspen.base.NoRetry
}

/** Provides a fully-functional AspenSystem for testing application-level operations
 *  Defaults to memory-only data store but may use other stores via the constructor param  
 */
class TestSystem(
    val storeFactory: (DataStoreID) => (BootstrapDataStore, CrashRecoveryLog) = TestSystem.memoryStoreFactory,
    val noRetry: RetryStrategy = TestSystem.NoRetry,
    val bootstrapPoolIDA: IDA = TestSystem.DefaultIDA,
    val systemTreeNodeSize: Int = TestSystem.DefaultSystemTreeNodeSize,
    val systemTreeNodeCacheFactory: (AspenSystem) => KVTreeNodeCache = TestSystem.noTreeNodeCacheFactory,
    val userTaskTypeRegistry: Option[TaskTypeRegistry] = None) {
  
  import scala.language.postfixOps
  import Bootstrap._
  
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
      crl: CrashRecoveryLog,
      net: TestNetwork,
      radiclePointer: ObjectPointer): (BasicAspenSystem, StorageNode) = {
    
    val clientId = ClientID(new UUID(0, store.storeId.poolIndex))
    
    val sys = new BasicAspenSystem(
        chooseDesignatedLeader = (o:ObjectPointer) => 0,
        isStorageNodeOnline = (_:StorageNodeID) => true,
        net = new net.CNet(clientId),
        defaultReadDriverFactory = BaseReadDriver.noErrorRecoveryReadDriver(ExecutionContext.Implicits.global) _,
        defaultTransactionDriverFactory = ClientTransactionDriver.noErrorRecoveryFactory,
        defaultAllocationDriverFactory = BaseAllocationDriver.NoErrorRecoveryAllocationDriver,
        transactionFactory = BaseTransaction.Factory,
        storagePoolFactory = BaseStoragePool.Factory,
        bootstrapPoolIDA = bootstrapPoolIDA,
        systemTreeNodeCacheFactory = systemTreeNodeCacheFactory,
        radiclePointer = radiclePointer,
        initializationRetryStrategy = noRetry,
        userTaskTypeRegistry = userTaskTypeRegistry
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
  
  val (store0, crl0) = storeFactory(DataStoreID(BootstrapStoragePoolUUID, 0))
  val (store1, crl1) = storeFactory(DataStoreID(BootstrapStoragePoolUUID, 1))
  val (store2, crl2) = storeFactory(DataStoreID(BootstrapStoragePoolUUID, 2))
  
  val radiclePointer = Await.result(Bootstrap.initializeNewSystem(List(store0, store1, store2), bootstrapPoolIDA), 500 milliseconds)
  
  val net = new TestNetwork
  
  val (sys0, sn0) = mkStorageNode(store0, crl0, net, radiclePointer)
  val (sys1, sn1) = mkStorageNode(store1, crl1, net, radiclePointer)
  val (sys2, sn2) = mkStorageNode(store2, crl2, net, radiclePointer)
  
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
    
    val faRegistry = FinalizationActionRegistry(noRetry, sys, kvTreeFactory)
    
    val finalizerFactory = new BaseTransactionFinalizer(sys, faRegistry)
     
    val txMgr = new StorageNodeTransactionManager(sn.crl, sn.net.transactionHandler, TransactionDriver.noErrorRecoveryFactory, finalizerFactory.factory)
    val allocMgr = new StorageNodeAllocationManager(sn.crl, sn.net.allocationHandler)
    
    sn.recoverPendingOperations(txMgr, allocMgr)
  }
  
  recover(sys0, sn0)
  recover(sys1, sn1)
  recover(sys2, sn2)
  
  val aspenSystem = sys0
}