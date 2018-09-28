package com.ibm.aspen.base

import java.util.UUID

import com.ibm.aspen.base.impl._
import com.ibm.aspen.core.allocation.{AllocationRecoveryState, BaseAllocationDriver}
import com.ibm.aspen.core.crl.{CrashRecoveryLog, MemoryOnlyCRL}
import com.ibm.aspen.core.data_store.{DataStore, DataStoreFrontend, DataStoreID, MemoryOnlyDataStoreBackend}
import com.ibm.aspen.core.ida.{IDA, Replication}
import com.ibm.aspen.core.network.{ClientID, TestNetwork}
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, ObjectPointer}
import com.ibm.aspen.core.read.BaseReadDriver
import com.ibm.aspen.core.transaction.{ClientTransactionDriver, TransactionDriver, TransactionRecoveryState}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object TestSystem {
  def memoryStoreFactory(storeId: DataStoreID): (DataStore, CrashRecoveryLog) = {
    val ds = new DataStoreFrontend(storeId, 
      new MemoryOnlyDataStoreBackend()(ExecutionContext.Implicits.global), Nil, Nil)
    (ds, new MemoryOnlyCRL)
  }

  val DefaultIDA = new Replication(3,2)
  
  val DefaultSystemTreeNodeSize = 2048
  
  val NoRetry = new AssertOnRetry //new com.ibm.aspen.base.NoRetry
}

/** Provides a fully-functional AspenSystem for testing application-level operations
 *  Defaults to memory-only data store but may use other stores via the constructor param  
 */
class TestSystem(
    val storeFactory: (DataStoreID) => (DataStore, CrashRecoveryLog) = TestSystem.memoryStoreFactory,
    val noRetry: RetryStrategy = TestSystem.NoRetry,
    val bootstrapPoolIDA: IDA = TestSystem.DefaultIDA,
    val systemTreeNodeSize: Int = TestSystem.DefaultSystemTreeNodeSize) {
  
  import Bootstrap._

  import scala.language.postfixOps
  
  var typeRegistries: List[TypeRegistry] = Nil
  
  def getRegistries() = synchronized { typeRegistries }
  
  def synchronousWaitForTransactionsComplete(ostack: Option[String]=None): Unit = {
    val stack = ostack match {
      case None => com.ibm.aspen.util.getStack()
      case Some(s) => s
    }
    var count = 0
    while (!sn0.allTransactionsComplete && !sn1.allTransactionsComplete && !sn2.allTransactionsComplete && count < 100) {
      count += 1
      Thread.sleep(5) 
    }
        
    if (count > 100) {
      throw new Exception(s"Finalization Actions Timed Out. Stack:\n$stack")
    }
  }
  
  def waitForTransactionsComplete(): Future[Unit] = {
    val stack = com.ibm.aspen.util.getStack()
    Future {
      synchronousWaitForTransactionsComplete(Some(stack))
    }
  }
  
  object userTypeRegistry extends TypeRegistry {
  
    def getTypeFactory[T <: TypeFactory](factoryUUID: UUID): Option[T] = {
      
      @tailrec
      def rfind(l: List[TypeRegistry]): Option[T] = if (l.isEmpty) None else {
        l.head.getTypeFactory[T](factoryUUID) match {
          case None => rfind(l.tail)
          case Some(tgt) => Some(tgt)
        }
      }
      
      rfind(typeRegistries)
    }
  }

  def mkStorageNode(
      store: DataStore, 
      crl: CrashRecoveryLog,
      net: TestNetwork,
      radiclePointer: KeyValueObjectPointer): (BasicAspenSystem, StorageNode) = {
    
    val clientId = ClientID(new UUID(0, store.storeId.poolIndex))
    
    object Host extends StorageHost {
    
      val uuid: UUID = UUID.randomUUID()
      
      def online: Boolean = true
    
      def ownsStore(storeId: DataStoreID)(implicit ec: ExecutionContext): Future[Boolean] = Future.successful(true)
    }
    
    val sys = new BasicAspenSystem(
        chooseDesignatedLeader = (o:ObjectPointer) => 0,
        getStorageHostFn = (_:DataStoreID) => Future.successful(Host),
        net = new net.CNet(clientId),
        defaultReadDriverFactory = BaseReadDriver.noErrorRecoveryReadDriver(ExecutionContext.Implicits.global) _,
        defaultTransactionDriverFactory = ClientTransactionDriver.noErrorRecoveryFactory,
        defaultAllocationDriverFactory = BaseAllocationDriver.NoErrorRecoveryAllocationDriver,
        transactionFactory = BaseTransaction.Factory,
        storagePoolFactory = BaseStoragePool.Factory,
        bootstrapPoolIDA = bootstrapPoolIDA,
        radiclePointer = radiclePointer,
        retryStrategy = noRetry,
        userTypeRegistry = Some(userTypeRegistry)
        )
    
    val storageNode = new StorageNode(sys, crl, new net.SNet)
    
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
  
  val missedUpdateStrategy = PerStoreMissedUpdate.getStrategy(Array(BootstrapObjectAllocaterUUID), Array(8192), Array(1000))
  
  val radiclePointer = Await.result(Bootstrap.initializeNewSystem(List(store0, store1, store2), bootstrapPoolIDA, missedUpdateStrategy), 500 milliseconds)
  
  val net = new TestNetwork
  
  val (sys0, sn0) = mkStorageNode(store0, crl0, net, radiclePointer)
  val (sys1, sn1) = mkStorageNode(store1, crl1, net, radiclePointer)
  val (sys2, sn2) = mkStorageNode(store2, crl2, net, radiclePointer)
  
  Await.result(sys0.radicle, 1000 milliseconds)
  Await.result(sys1.radicle, 1000 milliseconds)
  Await.result(sys2.radicle, 1000 milliseconds)
  
  def recover(sys: BasicAspenSystem, sn: StorageNode): Unit = {
    
    val faRegistry = BaseImplTypeRegistry(sys)
    
    val finalizerFactory = new BaseTransactionFinalizer(sys)
    
    def txcomplete(txuuid: UUID): Option[Boolean] = sys.transactionCache.getIfPresent(txuuid)
     
    val txMgr = new StorageNodeTransactionManager(sn.crl, txcomplete, sn.net.transactionHandler, TransactionDriver.noErrorRecoveryFactory, finalizerFactory.factory)
    val allocMgr = new StorageNodeAllocationManager(sn.crl, sn.net.allocationHandler)
    
    sn.recoverPendingOperations(txMgr, allocMgr)
  }
  
  recover(sys0, sn0)
  recover(sys1, sn1)
  recover(sys2, sn2)
  
  def shutdown(): Unit = {
    sys0.shutdown()
    sys1.shutdown()
    sys2.shutdown()
    Await.result(net.shutdown(), 1000 milliseconds)
    Await.result(Future.sequence(List(sn0.shutdown(), sn1.shutdown(), sn2.shutdown())), 1000 milliseconds)
  }
  
  val aspenSystem = sys0

  net.setSystem(aspenSystem)
}