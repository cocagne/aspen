package com.ibm.aspen.base.impl

import com.ibm.aspen.core.ida.IDA
import java.util.UUID
import com.ibm.aspen.core.network.StorageNodeID
import com.ibm.aspen.base.StoragePool
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.InsufficientOnlineNodes

class BaseStoragePool(
    val uuid: UUID,
    val hostingStorageNodes: Array[StorageNodeID],
    val allocationTreeDefinition: ObjectPointer,
    val isStorageNodeOnline: (StorageNodeID) => Boolean) extends StoragePool {
  
  def supportsIDA(ida: IDA): Boolean = ida.width >= numberOfStores
  
  def selectStoresForAllocation(ida: IDA): Array[Int] = {
    val availableIndicies = (0 until numberOfStores).foldLeft(List[Int]())((lst, idx) => if (isStorageNodeOnline(hostingStorageNodes(idx))) idx :: lst else lst)
    val randomizedIndices = scala.util.Random.shuffle(availableIndicies)
    val arr = randomizedIndices.take(ida.width).sorted.toArray
    if (arr.length < ida.width)
      throw new InsufficientOnlineNodes(ida.width, arr.length)
    arr
  }
  
}

object BaseStoragePool {
  
}