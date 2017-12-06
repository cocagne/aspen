package com.ibm.aspen.base.impl

import com.ibm.aspen.base.AspenSystem
import java.util.UUID
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.read.ClientReadManager
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import com.ibm.aspen.base.StoragePool
import com.ibm.aspen.core.read.ReadDriver
import com.ibm.aspen.base.ObjectStateAndData
import com.ibm.aspen.core.read.DataRetrievalFailed
import com.ibm.aspen.base.Transaction
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.core.transaction.ClientTransactionManager
import com.ibm.aspen.core.allocation.ClientAllocationManager
import com.ibm.aspen.core.allocation.AllocationDriver
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.network.StorageNodeID
import com.ibm.aspen.base.kvtree.KVTree
import com.ibm.aspen.base.kvtree.KVTreeNodeCache
import com.ibm.aspen.base.kvtree.KVTreeSimpleFactory
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.base.UnsupportedIDA
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.base.StoreAllocationError
import com.ibm.aspen.base.RetryStrategy
import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.core.Util
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.network.ClientSideNetwork



object BasicAspenSystem {
  
  import scala.language.implicitConversions
  
  implicit def uuid2byte(uuid: UUID): Array[Byte] = Util.uuid2byte(uuid)
  
  type TransactionFactory = (ClientTransactionManager,  
                             (ObjectPointer) => Byte, // Choose the designatedLeader for the Tx based on online/offline peer knowledge
                             Option[ClientTransactionDriver.Factory] // Strategy for driving the Tx to completion. Default will be used if None
                             ) => Transaction
}

// StoragePool UUID 0000 is used for bootstrapping pool
class BasicAspenSystem(
    val chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions
    val isStorageNodeOnline: (StorageNodeID) => Boolean,
    val net: ClientSideNetwork,
    val defaultReadDriverFactory: ReadDriver.Factory,
    val defaultTransactionDriverFactory: ClientTransactionDriver.Factory,
    val defaultAllocationDriverFactory: AllocationDriver.Factory,
    val transactionFactory: BasicAspenSystem.TransactionFactory,
    val storagePoolFactory: StoragePoolFactory,
    val bootstrapPoolIDA: IDA,
    val systemTreeNodeCacheFactory: (AspenSystem) => KVTreeNodeCache,
    val radiclePointer: ObjectPointer,
    val initializationRetryStrategy: RetryStrategy
    )(implicit ec: ExecutionContext) extends AspenSystem {
  
  import BasicAspenSystem._
  import Bootstrap._
  
  protected val readManager = new ClientReadManager(net.readHandler)
  protected val txManager = new ClientTransactionManager(net.transactionHandler, defaultTransactionDriverFactory)
  protected val allocManager = new ClientAllocationManager(net.allocationHandler, defaultAllocationDriverFactory)
  
  net.readHandler.setReceiver(readManager)
  net.transactionHandler.setReceiver(txManager)
  net.allocationHandler.setReceiver(allocManager)
  
  def clientId = net.clientId
  
  val systemTreeNodeCache = systemTreeNodeCacheFactory(this)
  val systemTreeFactory = new KVTreeSimpleFactory(this, SystemAllocationPolicyUUID, BootstrapStoragePoolUUID, bootstrapPoolIDA,
                                                  SystemTreeNodeSizeLimit, systemTreeNodeCache, SystemTreeKeyComparisonStrategy)
  
  val radicle: Future[Radicle] = initializationRetryStrategy.retryUntilSuccessful {
    readObject(radiclePointer) map { osd => BaseCodec.decodeRadicle(osd.data) }
  }
  
  val systemTree: Future[KVTree] = initializationRetryStrategy.retryUntilSuccessful {
    radicle.flatMap(r => systemTreeFactory.createTree(r.systemTreeDefinitionPointer))
  }
  
  val storagePoolTree: Future[KVTree] = initializationRetryStrategy.retryUntilSuccessful {
    for {
      sysTree <- systemTree
      oenc <- sysTree.get(StoragePoolTreeUUID)
      if oenc.isDefined
      poolTree <- systemTreeFactory.createTree(NetworkCodec.byteArrayToObjectPointer(oenc.get))
    } yield {
      poolTree
    }
  }
  
  def readObject(
      objectPointer:ObjectPointer, 
      readStrategy: Option[ReadDriver.Factory] ): Future[ObjectStateAndData] = readManager.read(objectPointer, true, false, 
          readStrategy.getOrElse(defaultReadDriverFactory)).map(r => r match {
            case Left(err) => throw err
            case Right(os) => os.data match {
              case Some(data) => ObjectStateAndData(objectPointer, os.revision, os.refcount, data)
              case None => throw DataRetrievalFailed()
            }
          })
          
  def newTransaction(): Transaction = transactionFactory(txManager, chooseDesignatedLeader, None)
  
  override def newTransaction(transactionDriverStrategy: ClientTransactionDriver.Factory): Transaction = {
    transactionFactory(txManager, chooseDesignatedLeader, Some(transactionDriverStrategy))
  }
  
  def allocateObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID, 
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: DataBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer] = {
    val encoded = objectIDA.encode(initialContent)
    val newObjectUUID = UUID.randomUUID()
    
    for {
      pool <- getStoragePool(poolUUID)
      hosts = pool.selectStoresForAllocation(objectIDA)
      objectData = hosts.map(_.asInstanceOf[Byte]).zip(encoded).toMap

      result <- allocManager.allocate(net.allocationHandler, poolUUID, newObjectUUID, objectSize, objectIDA, objectData, ObjectRefcount(0,1), 
                                      t.uuid, allocatingObject, allocatingObjectRevision)
    } yield {
      result match {
        case Left(errmap) => throw new StoreAllocationError(allocatingObject, allocatingObjectRevision, poolUUID, objectSize, objectIDA, errmap)
        case Right(newObjPtr) =>
          AllocationFinalizationAction.addToAllocationTree(t, pool.poolDefinitionPointer, newObjPtr)
          newObjPtr
      }
    }
  }
  
  def getStoragePool(poolUUID: UUID): Future[StoragePool] = for {
    spTree <- storagePoolTree
    encPtr <- spTree.get(poolUUID)
    if (encPtr.isDefined)
    poolPtr = NetworkCodec.byteArrayToObjectPointer(encPtr.get)
    pool <- getStoragePool(poolPtr)
  } yield pool
  
  def getStoragePool(storagePoolDefinitionPointer: ObjectPointer): Future[StoragePool] = { 
    storagePoolFactory.createStoragePool(this, storagePoolDefinitionPointer, isStorageNodeOnline)
  }
}
