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
import com.ibm.aspen.base.Transaction
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.core.transaction.ClientTransactionManager
import com.ibm.aspen.core.allocation.ClientAllocationManager
import com.ibm.aspen.core.allocation.AllocationDriver
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.base.UnsupportedIDA
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.base.StoreAllocationError
import com.ibm.aspen.base.RetryStrategy
import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.network.ClientSideNetwork
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.base.ObjectAllocater
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.base.task.TaskGroupExecutor
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.util.uuid2byte
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.read.FullObject
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.core.read.SingleKey
import com.ibm.aspen.core.read.LargestKeyLessThan
import com.ibm.aspen.core.read.KeyRange
import com.ibm.aspen.core.read.LargestKeyLessThanOrEqualTo
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.base.ObjectSizeExceeded
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.base.tieredlist.TieredKeyValueList
import com.ibm.aspen.base.AggregateTypeRegistry
import com.ibm.aspen.base.task.DurableTaskType
import com.ibm.aspen.base.TypeRegistry
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.task.TaskGroupType
import com.ibm.aspen.base.StorageHost
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.base.MissedUpdateStrategy
import com.ibm.aspen.base.MissedUpdateHandler
import com.ibm.aspen.base.MissedUpdateHandlerFactory
import com.ibm.aspen.base.MissedUpdateIterator
import com.ibm.aspen.base.ObjectAllocaterFactory
import org.apache.logging.log4j.scala.Logging
import com.ibm.aspen.base.tieredlist.TieredKeyValueListRoot
import com.ibm.aspen.base.tieredlist.MutableKeyValueObjectRootManager
import com.ibm.aspen.base.tieredlist.MutableTKVLRootManager
import com.github.blemale.scaffeine.Cache
import com.github.blemale.scaffeine.Scaffeine
import scala.concurrent.duration._
import com.ibm.aspen.base.ExponentialBackoffRetryStrategy


object BasicAspenSystem {
  
  import scala.language.implicitConversions
  
  import com.ibm.aspen.util.uuid2byte
  
  /** Used by allocation finalizers instead of the one passed in to the constructor.
   *  
   *  This is primarily used to avoid problems with the AssertOnRetry strategy as contention on the allocation tree
   *  is normal even during unit tests. Insert/Remove retries are necessary to work around the conflicts. When 
   *  AssertOnRetry is used, these retries are flagged as errors instead of normal operation.
   */
  val FinalizationActionRetryStrategyUUID = UUID.fromString("b32975fd-cdf2-41a8-891f-87d8ae179664")
  
  type TransactionFactory = (BasicAspenSystem,
                             ClientTransactionManager,  
                             (ObjectPointer) => Byte, // Choose the designatedLeader for the Tx based on online/offline peer knowledge
                             Option[ClientTransactionDriver.Factory] // Strategy for driving the Tx to completion. Default will be used if None
                             ) => Transaction
}

// StoragePool UUID 0000 is used for bootstrapping pool
class BasicAspenSystem(
    val chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions
    val getStorageHostFn: (DataStoreID) => Future[StorageHost],
    val net: ClientSideNetwork,
    val defaultReadDriverFactory: ReadDriver.Factory,
    val defaultTransactionDriverFactory: ClientTransactionDriver.Factory,
    val defaultAllocationDriverFactory: AllocationDriver.Factory,
    val transactionFactory: BasicAspenSystem.TransactionFactory,
    val storagePoolFactory: StoragePoolFactory,
    val bootstrapPoolIDA: IDA,
    val radiclePointer: KeyValueObjectPointer,
    val retryStrategy: RetryStrategy,
    userTypeRegistry: Option[TypeRegistry]
    )(implicit ec: ExecutionContext) extends AspenSystem with Logging {
  
  import BasicAspenSystem._
  import Bootstrap._
  
  logger.debug("Constructing BasicAspenSystem")

  private[this] var attributes: Map[String,String] = Map()
  
  val transactionCache: Cache[UUID,Boolean] = Scaffeine()
        .expireAfterWrite(Duration(5, MINUTES))
        .maximumSize(1000)
        .build[UUID, Boolean]()
        
  protected val readManager = new ClientReadManager(this, transactionCache.getIfPresent, net.readHandler)
  protected val txManager = new ClientTransactionManager(net.transactionHandler, defaultTransactionDriverFactory)
  protected val allocManager = new ClientAllocationManager(net.allocationHandler, defaultAllocationDriverFactory)
  
  
  protected var retryStrategies = Map[UUID, RetryStrategy](
      (FinalizationActionRetryStrategyUUID -> new ExponentialBackoffRetryStrategy(backoffLimit=10000, initialRetryDelay=3)))

  def getSystemAttribute(key: String): Option[String] = synchronized { attributes.get(key) }
  def setSystemAttribute(key: String, value: String): Unit = synchronized{ attributes += key -> value }

  def getRetryStrategy(uuid: UUID): RetryStrategy = synchronized {
    retryStrategies.getOrElse(uuid, retryStrategy)
  }
  
  def registerRetryStrategy(uuid: UUID, strategy: RetryStrategy): Unit = synchronized {
    retryStrategies +=  (uuid -> strategy)
  }
  
  def getStorageHost(storeId: DataStoreID): Future[StorageHost] = getStorageHostFn(storeId)
  
  /** Immediately cancels all future activity scheduled for execution */
  def shutdown(): Unit = {
    readManager.shutdown()
    txManager.shutdown()
    allocManager.shutdown()
    retryStrategy.shutdown()
  }
  
  val bootstrapPoolAllocater = new SinglePoolObjectAllocater(this, Bootstrap.BootstrapObjectAllocaterUUID, 
      Bootstrap.BootstrapStoragePoolUUID, None, bootstrapPoolIDA)
  
  net.readHandler.setReceiver(readManager)
  net.transactionHandler.setReceiver(txManager)
  net.allocationHandler.setReceiver(allocManager)
  
  def clientId = net.clientId
  
  val typeRegistry: TypeRegistry = userTypeRegistry match {
    case None => BaseImplTypeRegistry(this)
    case Some(utr) => new AggregateTypeRegistry( utr :: BaseImplTypeRegistry(this) :: Nil )
  }
  
  
  lazy val radicle: Future[KeyValueObjectState] = retryStrategy.retryUntilSuccessful {
    readObject(radiclePointer) 
  }
  
  lazy val systemTree: Future[MutableTieredKeyValueList] = retryStrategy.retryUntilSuccessful {
    radicle.map { kvos => 
      new MutableTieredKeyValueList(MutableKeyValueObjectRootManager(this, kvos, Bootstrap.SystemTreeKey))
    }
  }
  
  private[this] def loadSupportTree(treeUUID: UUID): Future[MutableTieredKeyValueList] = retryStrategy.retryUntilSuccessful {
    systemTree.flatMap { sysTree =>
      MutableTKVLRootManager.load(sysTree, treeUUID).map { orootMgr =>
        new MutableTieredKeyValueList(orootMgr.get)
      }
    }
  }
  
  lazy val storagePoolTree: Future[MutableTieredKeyValueList] = loadSupportTree(Bootstrap.StoragePoolTreeUUID) 
  
  lazy val taskGroupTree: Future[MutableTieredKeyValueList] = loadSupportTree(Bootstrap.TaskGroupTreeUUID)
  
  def readObject(
      objectPointer:DataObjectPointer, 
      readStrategy: Option[ReadDriver.Factory],
      disableOpportunisticRebuild:Boolean): Future[DataObjectState] = readManager.read(objectPointer, FullObject(), false, disableOpportunisticRebuild,
          readStrategy.getOrElse(defaultReadDriverFactory)).map(r => r match {
            case Left(err) => throw err
            case Right(os) => os.asInstanceOf[DataObjectState]
          })
          
  def readObject(
      pointer:KeyValueObjectPointer, 
      readStrategy: Option[ReadDriver.Factory],
      disableOpportunisticRebuild:Boolean): Future[KeyValueObjectState] = readManager.read(pointer, FullObject(), false, disableOpportunisticRebuild, 
          readStrategy.getOrElse(defaultReadDriverFactory)).map(r => r match {
            case Left(err) => throw err
            case Right(os) => os.asInstanceOf[KeyValueObjectState]
          })
          
  def readSingleKey(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = readManager.read(pointer, 
      SingleKey(key, comparison), false, false, defaultReadDriverFactory).map(r => r match {
        case Left(err) => throw err
        case Right(os) => os.asInstanceOf[KeyValueObjectState]
      })
      
  def readLargestKeyLessThan(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = readManager.read(pointer, 
      LargestKeyLessThan(key, comparison), false, false, defaultReadDriverFactory).map(r => r match {
        case Left(err) => throw err
        case Right(os) => os.asInstanceOf[KeyValueObjectState]
      })
      
  def readLargestKeyLessThanOrEqualTo(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = readManager.read(pointer, 
      LargestKeyLessThanOrEqualTo(key, comparison), false, false, defaultReadDriverFactory).map(r => r match {
        case Left(err) => throw err
        case Right(os) => os.asInstanceOf[KeyValueObjectState]
      })
      
  def readKeyRange(pointer: KeyValueObjectPointer, minimum: Key, maximum: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = readManager.read(pointer, 
      KeyRange(minimum, maximum, comparison), false, false, defaultReadDriverFactory).map(r => r match {
        case Left(err) => throw err
        case Right(os) => os.asInstanceOf[KeyValueObjectState]
      })
          
  def newTransaction(): Transaction = transactionFactory(this, txManager, chooseDesignatedLeader, None)
  
  override def newTransaction(transactionDriverStrategy: ClientTransactionDriver.Factory): Transaction = {
    transactionFactory(this, txManager, chooseDesignatedLeader, Some(transactionDriverStrategy))
  }
  
  def lowLevelAllocateDataObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID, 
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: DataBuffer,
      afterTimestamp: Option[HLCTimestamp])(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    
    val encoded = objectIDA.encode(initialContent)
    
    try {
      
      objectSize.foreach(maxSize => if (encoded(0).size > maxSize) throw new ObjectSizeExceeded(maxSize, encoded(0).size))
    
    } catch {
      case err: ObjectSizeExceeded => return Future.failed(err)
    }

    for {
      pool <- getStoragePool(poolUUID)
      
      result <- allocManager.allocateDataObject(net.allocationHandler, t, pool, objectSize, objectIDA, encoded, afterTimestamp, ObjectRefcount(0,1), 
                                                allocatingObject, allocatingObjectRevision)
    } yield {
      result match {
        case Left(errmap) => throw new StoreAllocationError(allocatingObject, allocatingObjectRevision, poolUUID, objectSize, objectIDA, errmap)
        case Right(newObjPtr) => newObjPtr
      }
    }
  }
  
  def lowLevelAllocateKeyValueObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: List[KeyValueOperation],
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer]  = {
    
    val encoded = KeyValueOperation.encode(initialContent, objectIDA)
    
    try {
      
      objectSize.foreach(maxSize => if (encoded(0).size > maxSize) throw new ObjectSizeExceeded(maxSize, encoded(0).size))
    
    } catch {
      case err: ObjectSizeExceeded => return Future.failed(err)
    }
    
    for {
      pool <- getStoragePool(poolUUID)
      
      result <- allocManager.allocateKeyValueObject(net.allocationHandler, t, pool, objectSize, objectIDA, afterTimestamp, ObjectRefcount(0,1),
                                                    allocatingObject, allocatingObjectRevision, encoded)
    } yield {
      result match {
        case Left(errmap) => throw new StoreAllocationError(allocatingObject, allocatingObjectRevision, poolUUID, objectSize, objectIDA, errmap)
        case Right(newObjPtr) => newObjPtr
      }
    }
  }
  
  def getStoragePool(poolUUID: UUID): Future[StoragePool] = {
    for {
      spTree <- storagePoolTree
      v <- spTree.get(poolUUID)
      if (v.isDefined)
      pool <- getStoragePool(KeyValueObjectPointer(v.get.value))
    } yield pool
  }
  
  def getStoragePool(storagePoolDefinitionPointer: KeyValueObjectPointer): Future[StoragePool] = { 
    storagePoolFactory.createStoragePool(this, storagePoolDefinitionPointer)
  }
  
  def createMissedUpdateHandler(
      mus: MissedUpdateStrategy,
      pointer: ObjectPointer, 
      missedStores: List[Byte])(implicit ec: ExecutionContext): MissedUpdateHandler = {
    typeRegistry.getTypeFactory[MissedUpdateHandlerFactory](mus.strategyUUID) match {
      case None => throw new Exception(s"Invalid Missed Update Strategy ${mus.strategyUUID}")
      case Some(f) => f.createHandler(mus, this, pointer, missedStores)
    }
  }
  
  def createMissedUpdateIterator(
      mus: MissedUpdateStrategy, 
      storeId: DataStoreID)(implicit ec: ExecutionContext): MissedUpdateIterator = {
    typeRegistry.getTypeFactory[MissedUpdateHandlerFactory](mus.strategyUUID) match {
      case None => throw new Exception(s"Invalid Missed Update Strategy ${mus.strategyUUID}")
      case Some(f) => f.createIterator(mus, this, storeId)
    }
  }
  
  // TODO: Implement in terms of tree, allocater type registry, & save/restore
  //def getObjectAllocater(allocaterUUID: UUID): Future[ObjectAllocater] = Future.successful(new SinglePoolObjectAllocater(this, 
  //    Bootstrap.BootstrapObjectAllocaterUUID, Bootstrap.BootstrapStoragePoolUUID, None, bootstrapPoolIDA))
      
  def getObjectAllocater(allocaterUUID: UUID): Future[ObjectAllocater] = {
    typeRegistry.getTypeFactory[ObjectAllocaterFactory](allocaterUUID) match {
      case None =>
        println(s"*************** Unknown Allocater UUID: $allocaterUUID")
        com.ibm.aspen.util.printStack()
        println("*********************************************************************")
        Future.failed(new Exception(s"Unknown Object Allocater: ${allocaterUUID}"))
      case Some(f) => f.create(this)
    }
  }
}
