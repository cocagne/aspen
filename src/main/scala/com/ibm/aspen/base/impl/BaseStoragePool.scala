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
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.base.tieredlist.TieredKeyValueList
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.base.tieredlist.SimpleMutableTieredKeyValueList
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering

object BaseStoragePool {
  
  val PoolUUIDKey            = Key(Array[Byte](0))
  val HostingStorageNodesKey = Key(Array[Byte](1))
  val AllocationTreeKey      = Key(Array[Byte](2))
  
  def encodeStorageNodeIDs(ids: Array[StorageNodeID]): Array[Byte] = {
    val arr = new Array[Byte](16 * ids.length)
    val bb = ByteBuffer.wrap(arr)
    ids.foreach { i =>
      bb.putLong(i.uuid.getMostSignificantBits)
      bb.putLong(i.uuid.getLeastSignificantBits)
    }
    arr
  }
  def decodeStorageNodeIDs(arr: Array[Byte]): Array[StorageNodeID] = {
    val ids = new Array[StorageNodeID](arr.length/16)
    val bb = ByteBuffer.wrap(arr)
    for (i <- 0 until ids.length) {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      ids(i) = StorageNodeID(new UUID(msb, lsb))
    }
    ids
  }
  def arr2uuid(arr: Array[Byte]): UUID = {
    val bb = ByteBuffer.wrap(arr)
    val msb = bb.getLong()
    val lsb = bb.getLong()
    new UUID(msb, lsb)
  }
  
  object Factory extends StoragePoolFactory {
    def createStoragePool(
        system: AspenSystem, 
        poolDefinitionPointer: KeyValueObjectPointer, 
        isStorageNodeOnline: (StorageNodeID) => Boolean)(implicit ec: ExecutionContext): Future[BaseStoragePool] = {
      system.readObject(poolDefinitionPointer) map { 
        kvos =>
          val poolUUID = arr2uuid(kvos.contents(PoolUUIDKey).value)
          val hostingStorageNodes = decodeStorageNodeIDs(kvos.contents(HostingStorageNodesKey).value)
          val allocationTreeRoot = TieredKeyValueList.Root(kvos.contents(AllocationTreeKey).value)
            
          new BaseStoragePool(system, poolDefinitionPointer, kvos.revision, kvos.refcount, 
              poolUUID, hostingStorageNodes, allocationTreeRoot, isStorageNodeOnline)
      } 
    }
  }
}

class BaseStoragePool(
    val system: AspenSystem,
    val poolDefinitionPointer: KeyValueObjectPointer,
    val poolDefinitionRevision: ObjectRevision,
    val poolDefinitionRefcount: ObjectRefcount,
    val uuid: UUID,
    val hostingStorageNodes: Array[StorageNodeID],
    val allocationTreeRoot: TieredKeyValueList.Root,
    val isStorageNodeOnline: (StorageNodeID) => Boolean) extends StoragePool {
  
  import BaseStoragePool._
  
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
  
  def refresh()(implicit ec: ExecutionContext): Future[StoragePool] = Factory.createStoragePool(system, poolDefinitionPointer, isStorageNodeOnline)
  
  override def getAllocationTree(retryStrategy: RetryStrategy)(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList] = {
    Future.successful(new SimpleMutableTieredKeyValueList(system, Left(poolDefinitionPointer), AllocationTreeKey, ByteArrayKeyOrdering))
  }

} // end BaseStoragePool

