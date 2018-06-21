package com.ibm.aspen.base.impl

import com.ibm.aspen.core.ida.IDA
import java.util.UUID
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
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.base.tieredlist.TieredKeyValueList
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.base.tieredlist.SimpleMutableTieredKeyValueList
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering
import com.ibm.aspen.util.byte2uuid
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.base.StorageHost

object BaseStoragePool {
  
  val PoolUUIDKey            = Key(Array[Byte](0))
  val NumberOfStoresKey      = Key(Array[Byte](1))
  val AllocationTreeKey      = Key(Array[Byte](2))
  
  def encodeNumberOfStores(num: Int): Array[Byte] = {
    val arr = new Array[Byte](4)
    val bb = ByteBuffer.wrap(arr)
    bb.putInt(num)
    arr
  }
  
  object Factory extends StoragePoolFactory {
    def createStoragePool(
        system: AspenSystem, 
        poolDefinitionPointer: KeyValueObjectPointer)(implicit ec: ExecutionContext): Future[BaseStoragePool] = {
      system.readObject(poolDefinitionPointer) flatMap { 
        kvos =>
          val poolUUID = byte2uuid(kvos.contents(PoolUUIDKey).value)
          val numStores = ByteBuffer.wrap(kvos.contents(NumberOfStoresKey).value).getInt()
          val allocationTreeRoot = TieredKeyValueList.Root(kvos.contents(AllocationTreeKey).value)
          
          val fhosts = (0 until numStores).map { i =>
            val storeId = DataStoreID(poolUUID, i.asInstanceOf[Byte])
            system.getStorageHost(storeId)
          }
          
          Future.sequence(fhosts).map { lst =>
          
          new BaseStoragePool(system, poolDefinitionPointer, kvos.revision, kvos.refcount, 
              poolUUID, lst.toArray, allocationTreeRoot)
          }
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
    val storageHosts: Array[StorageHost],
    val allocationTreeRoot: TieredKeyValueList.Root) extends StoragePool {
  
  import BaseStoragePool._
  
  override def supportsIDA(ida: IDA): Boolean = ida.width >= numberOfStores
  
  override def selectStoresForAllocation(ida: IDA): Array[Int] = {
    if (!supportsIDA(ida))
      throw new UnsupportedIDA(uuid, ida)
    
    val availableIndicies = (0 until numberOfStores).foldLeft(List[Int]())((lst, idx) => if (storageHosts(idx).online) idx :: lst else lst)
    val randomizedIndices = scala.util.Random.shuffle(availableIndicies)
    val arr = randomizedIndices.take(ida.width).sorted.toArray
    if (arr.length < ida.width)
      throw new InsufficientOnlineNodes(ida.width, arr.length)
    arr
  }
  
  def refresh()(implicit ec: ExecutionContext): Future[StoragePool] = Factory.createStoragePool(system, poolDefinitionPointer)
  
  override def getAllocationTree(retryStrategy: RetryStrategy)(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList] = {
    Future.successful(new SimpleMutableTieredKeyValueList(system, Left(poolDefinitionPointer), AllocationTreeKey, ByteArrayKeyOrdering))
  }

} // end BaseStoragePool

