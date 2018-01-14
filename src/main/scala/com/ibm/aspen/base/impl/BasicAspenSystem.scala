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
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.base.ObjectAllocater
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.base.TaskTypeRegistry
import com.ibm.aspen.base.TaskGroupTypeRegistry
import com.ibm.aspen.base.AggregateTaskGroupTypeRegistry
import com.ibm.aspen.base.impl.task.BaseTaskGroupTypeRegistry
import com.ibm.aspen.base.AggregateTaskTypeRegistry
import com.ibm.aspen.base.TaskGroup
import com.ibm.aspen.base.impl.task.TaskCodec
import com.ibm.aspen.base.TaskGroupExecutor
import com.ibm.aspen.base.FinalizationActionHandlerRegistry
import com.ibm.aspen.base.AggregateFinalizationActionHandlerRegistry
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.DataObjectPointer



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
    val radiclePointer: DataObjectPointer,
    val initializationRetryStrategy: RetryStrategy,
    userTaskTypeRegistry: Option[TaskTypeRegistry] = None,
    userTaskGroupTypeRegistry: Option[TaskGroupTypeRegistry] = None,
    userFinalizationActionHandlerRegistry: Option[FinalizationActionHandlerRegistry] = None,
    )(implicit ec: ExecutionContext) extends AspenSystem {
  
  import BasicAspenSystem._
  import Bootstrap._
  
  protected val readManager = new ClientReadManager(net.readHandler)
  protected val txManager = new ClientTransactionManager(net.transactionHandler, defaultTransactionDriverFactory)
  protected val allocManager = new ClientAllocationManager(net.allocationHandler, defaultAllocationDriverFactory)
  
  val bootstrapPoolAllocater = new SinglePoolObjectAllocater(this, Bootstrap.BootstrapStoragePoolUUID, None, bootstrapPoolIDA)
  
  net.readHandler.setReceiver(readManager)
  net.transactionHandler.setReceiver(txManager)
  net.allocationHandler.setReceiver(allocManager)
  
  def clientId = net.clientId
  
  val systemTreeNodeCache = systemTreeNodeCacheFactory(this)
  val systemTreeFactory = new KVTreeSimpleFactory(this, SystemAllocationPolicyUUID, BootstrapStoragePoolUUID, bootstrapPoolIDA,
                                                  SystemTreeNodeSizeLimit, systemTreeNodeCache, SystemTreeKeyComparisonStrategy)
  
  protected val taskTypeRegistry = userTaskTypeRegistry match {
    case None => new AggregateTaskTypeRegistry( Nil )
    case Some(registry) => new AggregateTaskTypeRegistry( registry :: Nil )
  }
  protected val taskGroupTypeRegistry = userTaskGroupTypeRegistry match {
    case None => BaseTaskGroupTypeRegistry
    case Some(registry) => new AggregateTaskGroupTypeRegistry( registry :: BaseTaskGroupTypeRegistry :: Nil )
  }
  protected val finalizationActionHandlerRegistry = {
    val baseRegistry = BaseFinalizationActionHandlerRegistry(initializationRetryStrategy, this, systemTreeFactory)
    
    userFinalizationActionHandlerRegistry match {
      case None => baseRegistry
      case Some(registry) => new AggregateFinalizationActionHandlerRegistry( registry :: baseRegistry :: Nil)
    }
  }
  
  lazy val radicle: Future[Radicle] = initializationRetryStrategy.retryUntilSuccessful {
    readObject(radiclePointer) map { osd => BaseCodec.decodeRadicle(osd.data) }
  }
  
  lazy val systemTree: Future[KVTree] = initializationRetryStrategy.retryUntilSuccessful {
    radicle.flatMap(r => systemTreeFactory.createTree(r.systemTreeDefinitionPointer))
  }
  
  lazy val storagePoolTree: Future[KVTree] = initializationRetryStrategy.retryUntilSuccessful {
    for {
      sysTree <- systemTree
      oenc <- sysTree.get(StoragePoolTreeUUID)
      if oenc.isDefined
      poolTree <- systemTreeFactory.createTree(NetworkCodec.byteArrayToObjectPointer(oenc.get).asInstanceOf[DataObjectPointer])
    } yield {
      poolTree
    }
  }
  
  lazy val taskGroupTree: Future[KVTree] = initializationRetryStrategy.retryUntilSuccessful {
    systemTree.flatMap { stree =>
      stree.get(TaskGroupTreeUUID).flatMap { oval =>
        oval match {
          case Some(enc) => systemTreeFactory.createTree(NetworkCodec.byteArrayToObjectPointer(enc).asInstanceOf[DataObjectPointer])
          
          case None =>
            val tdef = KVTree.defineNewTree(SystemAllocationPolicyUUID, SystemTreeKeyComparisonStrategy)
            implicit val tx = newTransaction()
            
            var treePointer: Option[DataObjectPointer] = None
            
            def createValue(allocatingObject: ObjectPointer, allocatingObjectRevision: ObjectRevision): Future[Array[Byte]] = {
              bootstrapPoolAllocater.allocateObject(allocatingObject, allocatingObjectRevision, tdef) map {
                ptr =>
                  treePointer = Some(ptr)
                  NetworkCodec.objectPointerToByteArray(ptr)
              }
            }
            
            
            stree.putGeneratedValueIntoTreeNode(TaskGroupTreeUUID, createValue) onComplete {
              case Failure(reason) => tx.invalidateTransaction(reason)
              case Success(_) => tx.commit()
            }
            
            tx.result.flatMap { _ => 
              treePointer match {
                case Some(ptr) => systemTreeFactory.createTree(ptr)
                case None => Future.failed(new Exception("Tree Not Created"))
              }
            }
        }
      }
    }
  }
  
  def readObject(
      objectPointer:DataObjectPointer, 
      readStrategy: Option[ReadDriver.Factory] ): Future[ObjectStateAndData] = readManager.read(objectPointer, true, false, 
          readStrategy.getOrElse(defaultReadDriverFactory)).map(r => r match {
            case Left(err) => throw err
            case Right(os) => os.data match {
              case Some(data) => ObjectStateAndData(objectPointer, os.revision, os.refcount, os.timestamp, data)
              case None => throw DataRetrievalFailed()
            }
          })
          
  def newTransaction(): Transaction = transactionFactory(txManager, chooseDesignatedLeader, None)
  
  override def newTransaction(transactionDriverStrategy: ClientTransactionDriver.Factory): Transaction = {
    transactionFactory(txManager, chooseDesignatedLeader, Some(transactionDriverStrategy))
  }
  
  def lowLevelAllocateObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID, 
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: DataBuffer,
      afterTimestamp: Option[HLCTimestamp])(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    val encoded = objectIDA.encode(initialContent)
    val newObjectUUID = UUID.randomUUID()
    val timestamp = afterTimestamp match {
      case None => HLCTimestamp.now
      case Some(ts) => HLCTimestamp.happensAfter(ts)
    }
    
    for {
      pool <- getStoragePool(poolUUID)
      hosts = pool.selectStoresForAllocation(objectIDA)
      objectData = hosts.map(_.asInstanceOf[Byte]).zip(encoded).toMap

      result <- allocManager.allocate(net.allocationHandler, poolUUID, newObjectUUID, objectSize, objectIDA, objectData, timestamp, ObjectRefcount(0,1), 
                                      t.uuid, allocatingObject, allocatingObjectRevision)
    } yield {
      result match {
        case Left(errmap) => throw new StoreAllocationError(allocatingObject, allocatingObjectRevision, poolUUID, objectSize, objectIDA, errmap)
        case Right(newObjPtr) =>
          AllocationFinalizationAction.addToAllocationTree(t, pool.poolDefinitionPointer, newObjPtr)
          newObjPtr.asInstanceOf[DataObjectPointer]
      }
    }
  }
  
  def getStoragePool(poolUUID: UUID): Future[StoragePool] = for {
    spTree <- storagePoolTree
    encPtr <- spTree.get(poolUUID)
    if (encPtr.isDefined)
    poolPtr = NetworkCodec.byteArrayToObjectPointer(encPtr.get).asInstanceOf[DataObjectPointer]
    pool <- getStoragePool(poolPtr)
  } yield pool
  
  def getStoragePoolAllocationTree(poolUUID: UUID, retryStrategy: RetryStrategy): Future[KVTree] = for {
    pool <- getStoragePool(poolUUID)
    allocTreePtr <- pool.getAllocationTreeDefinitionPointer(retryStrategy)
    allocTree <- systemTreeFactory.createTree(allocTreePtr)
  } yield {
    allocTree
  }
  
  def getStoragePool(storagePoolDefinitionPointer: DataObjectPointer): Future[StoragePool] = { 
    storagePoolFactory.createStoragePool(this, storagePoolDefinitionPointer, isStorageNodeOnline)
  }
  
  def createTaskGroup(groupUUID: UUID, taskGroupType: UUID, groupDefinitionContent: DataBuffer): Future[TaskGroup] = {
    implicit val tx = newTransaction()
    
    // TODO: Fail on group already exists
    
    // The supplied object pointer and revision are to the KVTree node the allocated object will be written into
    def createValue(allocatingObject: ObjectPointer, allocatingObjectRevision: ObjectRevision): Future[Array[Byte]] = {
      bootstrapPoolAllocater.allocateObject(allocatingObject, allocatingObjectRevision, groupDefinitionContent) map {
        ptr => TaskCodec.encodeTaskGroupTreeEntry(taskGroupType, ptr)
      }
    }
    
    for {
      tgTree <- taskGroupTree
      readyToCommit <- tgTree.putGeneratedValueIntoTreeNode(groupUUID, createValue)
      complete <- tx.commit()
      tg <- getTaskGroup(groupUUID)
    } yield {
      tg
    }
  }
  
  def getTaskGroup(groupUUID: UUID): Future[TaskGroup] = {
    
    def createGroup(entry: Array[Byte]): Future[TaskGroup] = {
      val (groupTypeUUID, groupDefinitionPointer) = TaskCodec.decodeTaskGroupTreeEntry(entry)
      
      taskGroupTypeRegistry.getTaskGroupType(groupTypeUUID) match {
        case None => Future.failed(new Exception("Missing Task Group Type!"))
        
        case Some(tgt) => tgt.createTaskGroup(this, groupUUID, groupDefinitionPointer)
      }
    }
    
    for {
      tgTree <- taskGroupTree
      ovalue <- tgTree.get(groupUUID)
      tg <- createGroup(ovalue.get)
    } yield {
      tg
    }
  }
  
  def createTaskGroupExecutor(groupUUID: UUID): Future[TaskGroupExecutor] = {
    def createGroup(entry: Array[Byte]): Future[TaskGroupExecutor] = {
      val (groupTypeUUID, groupDefinitionPointer) = TaskCodec.decodeTaskGroupTreeEntry(entry)
      
      taskGroupTypeRegistry.getTaskGroupType(groupTypeUUID) match {
        case None => Future.failed(new Exception("Missing Task Group Type!"))
        
        case Some(tgt) =>  
          tgt.createTaskGroupExecutor(this, groupUUID, groupDefinitionPointer, taskTypeRegistry, initializationRetryStrategy, bootstrapPoolAllocater)
      }
    }
    
    for {
      tgTree <- taskGroupTree
      ovalue <- tgTree.get(groupUUID)
      tg <- createGroup(ovalue.get)
    } yield {
      tg
    }
  }

}
