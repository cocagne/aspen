package com.ibm.aspen.base.impl

import com.ibm.aspen.core.ida.IDA
import java.util.UUID
import com.ibm.aspen.core.network.StorageNodeID
import com.ibm.aspen.base.StoragePool
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.InsufficientOnlineNodes
import com.ibm.aspen.base.AspenSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.base.UnsupportedIDA
import com.ibm.aspen.base.RetryStrategy
import java.nio.ByteBuffer
import com.ibm.aspen.base.kvtree.KVTree
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.DataObjectPointer

class BaseStoragePool(
    val system: AspenSystem,
    val poolDefinitionPointer: DataObjectPointer,
    val poolDefinitionRevision: ObjectRevision,
    val poolDefinitionRefcount: ObjectRefcount,
    val uuid: UUID,
    val hostingStorageNodes: Array[StorageNodeID],
    val allocationTreeDefinitionPointer: Option[DataObjectPointer],
    val isStorageNodeOnline: (StorageNodeID) => Boolean) extends StoragePool {
  
  override def supportsIDA(ida: IDA): Boolean = ida.width >= numberOfStores
  
  override def selectStoresForAllocation(ida: IDA): Array[Int] = {
    if (!supportsIDA(ida))
      throw new UnsupportedIDA(uuid, ida)
    
    val availableIndicies = (0 until numberOfStores).foldLeft(List[Int]())((lst, idx) => if (isStorageNodeOnline(hostingStorageNodes(idx))) idx :: lst else lst)
    val randomizedIndices = scala.util.Random.shuffle(availableIndicies)
    val arr = randomizedIndices.take(ida.width).sorted.toArray
    if (arr.length < ida.width)
      throw new InsufficientOnlineNodes(ida.width, arr.length)
    arr
  }
  
  def refresh()(implicit ec: ExecutionContext): Future[StoragePool] = BaseStoragePool.Factory.createStoragePool(system, poolDefinitionPointer, isStorageNodeOnline)
  
  override def getAllocationTreeDefinitionPointer(
      retryStrategy: RetryStrategy)
      (implicit ec: ExecutionContext): Future[DataObjectPointer] = {
    
    def createTree(currentPool: BaseStoragePool): Future[DataObjectPointer] = currentPool.allocationTreeDefinitionPointer match {
          case Some(allocTreePtr) => Future.successful(allocTreePtr)
          case None =>
            implicit val tx = system.newTransaction()
            
            val treeDefinitionContent = KVTree.defineNewTree(new UUID(0,0), KVTree.KeyComparison.Raw)
            
            for {
              allocTreePtr <- system.lowLevelAllocateDataObject(currentPool.poolDefinitionPointer, currentPool.poolDefinitionRevision, uuid, None, poolDefinitionPointer.ida, DataBuffer(treeDefinitionContent))
              newContent = BaseCodec.encodeStoragePoolDefinition(uuid, hostingStorageNodes, Some(allocTreePtr))
              _ = tx.overwrite(currentPool.poolDefinitionPointer, currentPool.poolDefinitionRevision, newContent)
              committed <- tx.commit()
            } yield allocTreePtr
    }
    
    allocationTreeDefinitionPointer match {
      case Some(allocTreePtr) => Future.successful(allocTreePtr)
      case None =>
        retryStrategy.retryUntilSuccessful {
          BaseStoragePool.Factory.createStoragePool(system, poolDefinitionPointer, isStorageNodeOnline) flatMap createTree
        }
    }
  }
  
} // end BaseStoragePool

object BaseStoragePool {
 
  object Factory extends StoragePoolFactory {
    def createStoragePool(
        system: AspenSystem, 
        poolDefinitionPointer: DataObjectPointer, 
        isStorageNodeOnline: (StorageNodeID) => Boolean)(implicit ec: ExecutionContext): Future[BaseStoragePool] = {
      system.readObject(poolDefinitionPointer) map { 
        osd => 
          val (poolUUID, hostingStorageNodes, allocationTreeDefinition) = BaseCodec.decodeStoragePoolDefinition(osd.data)
          new BaseStoragePool(system, poolDefinitionPointer, osd.revision, osd.refcount, 
              poolUUID, hostingStorageNodes, allocationTreeDefinition, isStorageNodeOnline)
      } 
    }
  }
}