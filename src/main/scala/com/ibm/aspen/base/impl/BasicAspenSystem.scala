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


object BasicAspenSystem {
  val SystemAllocationPolicyUUID = new UUID(0,0)
  val BootstrapStoragePoolUUID = new UUID(0,0)
  val SystemTreeNodeSizeLimit = 64 * 1024
  val SystemTreeKeyComparisonStrategy = KVTree.KeyComparison.Raw
  
  import scala.language.implicitConversions
  
  implicit def uuid2byte(uuid: UUID): Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(0, uuid.getMostSignificantBits)
    bb.putLong(8, uuid.getLeastSignificantBits)
    bb.array()
  }
}

// StoragePool UUID 0000 is used for bootstrapping pool
class BasicAspenSystem(
    val chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions
    val isStorageNodeOnline: (StorageNodeID) => Boolean,
    val messenger: ClientMessenger,
    val defaultReadDriverFactory: ReadDriver.Factory,
    val defaultTransactionDriverFactory: ClientTransactionDriver.Factory,
    val defaultAllocationDriverFactory: AllocationDriver.Factory,
    val transactionFactory: (BasicAspenSystem) => Transaction,
    val storagePoolFactory: StoragePoolFactory,
    val storagePoolTreeDefinition: ObjectPointer,
    val bootstrapPoolIDA: IDA,
    val systemTreeNodeCacheFactory: (AspenSystem) => KVTreeNodeCache
    // Probably dont need this. Network should be able to figure it out... val boostrapStorageNodes: Array[StorageNodeID] // Storage nodes ids for each of the stores in the bootstrap pool
    )(implicit ec: ExecutionContext) extends AspenSystem {
  
  import BasicAspenSystem._
  
  val readManager = new ClientReadManager(messenger)
  val txManager = new ClientTransactionManager(messenger, chooseDesignatedLeader, defaultTransactionDriverFactory)
  val allocManager = new ClientAllocationManager(messenger, defaultAllocationDriverFactory)
  
  messenger.setMessageReceivers(txManager, readManager, allocManager)
  
  def client = messenger.client
  
  val systemTreeNodeCache = systemTreeNodeCacheFactory(this)
  val systemTreeFactory = new KVTreeSimpleFactory(this, SystemAllocationPolicyUUID, BootstrapStoragePoolUUID, bootstrapPoolIDA,
                                                  SystemTreeNodeSizeLimit, systemTreeNodeCache, SystemTreeKeyComparisonStrategy)
  
  // TODO: Add a retry strategy to ensure this eventually succeeds
  val storagePoolTree: Future[KVTree] = systemTreeFactory.createTree(storagePoolTreeDefinition)
  
  def readObject(
      objectPointer:ObjectPointer, 
      readStrategy: Option[ReadDriver.Factory] ): Future[ObjectStateAndData] = readManager.read(objectPointer, true, false, 
          readStrategy.getOrElse(defaultReadDriverFactory)).map(r => r match {
            case Left(err) => throw err
            case Right(os) => os.data match {
              case Some(data) => ObjectStateAndData(objectPointer, os.revision, os.refcount, data.asReadOnlyBuffer())
              case None => throw DataRetrievalFailed()
            }
          })
          
  def newTransaction(): Transaction = transactionFactory(this)
  
  def allocateObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID, 
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: ByteBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer] = {
    
    val encoded = objectIDA.encode(initialContent)
    val newObjectUUID = UUID.randomUUID()
    
    for {
      pool <- getStoragePool(poolUUID)
      hosts = pool.selectStoresForAllocation(objectIDA)
      objectData = hosts.map(_.asInstanceOf[Byte]).zip(encoded).toMap

      result <- allocManager.allocate(messenger, poolUUID, newObjectUUID, objectSize, objectIDA, objectData, ObjectRefcount(0,1), 
                                      t.uuid, allocatingObject, allocatingObjectRevision)
    } yield {
     result match {
       case Left(errmap) => throw new StoreAllocationError(allocatingObject, allocatingObjectRevision, poolUUID, objectSize, objectIDA, errmap)
       case Right(newObjPtr) =>
         
         *** TODO: Add Allocation Finalizer to transaction. Pool has pointer to allocation tree
         
         newObjPtr
     }
    }
  }
  
  
  def getStoragePool(poolUUID: UUID): Future[StoragePool] = for {
    spTree <- storagePoolTree
    encPtr <- spTree.get(poolUUID)
    if (encPtr.isDefined)
    poolPtr = NetworkCodec.byteArrayToObjectPointer(encPtr.get)
    pool <- storagePoolFactory.createStoragePool(this, poolPtr, isStorageNodeOnline)
  } yield pool
  
}
