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

class BaseStoragePool(
    val system: AspenSystem,
    val poolPointer: ObjectPointer,
    val poolRevision: ObjectRevision,
    val poolRefcount: ObjectRefcount,
    val uuid: UUID,
    val hostingStorageNodes: Array[StorageNodeID],
    initialAllocationTreeDefinitionPointer: Option[ObjectPointer],
    val isStorageNodeOnline: (StorageNodeID) => Boolean) extends StoragePool {
  
  def supportsIDA(ida: IDA): Boolean = ida.width >= numberOfStores
  
  def selectStoresForAllocation(ida: IDA): Array[Int] = {
    if (!supportsIDA(ida))
      throw new UnsupportedIDA(uuid, ida)
    
    val availableIndicies = (0 until numberOfStores).foldLeft(List[Int]())((lst, idx) => if (isStorageNodeOnline(hostingStorageNodes(idx))) idx :: lst else lst)
    val randomizedIndices = scala.util.Random.shuffle(availableIndicies)
    val arr = randomizedIndices.take(ida.width).sorted.toArray
    if (arr.length < ida.width)
      throw new InsufficientOnlineNodes(ida.width, arr.length)
    arr
  }
  
  
  def allocationTreeDefinitionPointer()(implicit ec: ExecutionContext): Future[ObjectPointer] = Future.failed(new Exception("TODO"))
  
}

object BaseStoragePool {
 
  object Factory extends StoragePoolFactory {
    def createStoragePool(
        system: AspenSystem, 
        poolDefinitionPointer: ObjectPointer, 
        isStorageNodeOnline: (StorageNodeID) => Boolean)(implicit ec: ExecutionContext): Future[StoragePool] = {
      system.readObject(poolDefinitionPointer) map { 
        osd => 
          val (poolUUID, hostingStorageNodes, allocationTreeDefinition) = StoragePoolCodec.decode(osd.data)
          new BaseStoragePool(system, poolDefinitionPointer, osd.revision, osd.refcount, 
              poolUUID, hostingStorageNodes, allocationTreeDefinition, isStorageNodeOnline)
      } 
    }
  }
}