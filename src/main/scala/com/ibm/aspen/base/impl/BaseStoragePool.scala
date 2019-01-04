package com.ibm.aspen.base.impl

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.aspen.base._
import com.ibm.aspen.base.tieredlist.{MutableKeyValueObjectRootManager, MutableTieredKeyValueList, TieredKeyValueListIterator, TieredKeyValueListMutableRootManager}
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, ObjectPointer, ObjectRefcount, ObjectRevision}
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.util.byte2uuid

import scala.concurrent.{ExecutionContext, Future}

object BaseStoragePool {
  
  val PoolUUIDKey                 = Key(Array[Byte](0))
  val NumberOfStoresKey           = Key(Array[Byte](1))
  val AllocationTreeKey           = Key(Array[Byte](2))
  val MissedUpdateStrategyUUIDKey = Key(Array[Byte](3))
  val MissedUpdateStrategyCfgKey  = Key(Array[Byte](4))
  
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
          val allocationTreeRootManager = MutableKeyValueObjectRootManager(system, kvos, AllocationTreeKey)
          val strategyUUID = byte2uuid(kvos.contents(MissedUpdateStrategyUUIDKey).value)
          val cfg = kvos.contents.get(MissedUpdateStrategyCfgKey).map(v => v.value)
          val mus = MissedUpdateStrategy(strategyUUID, cfg)
          
          val fhosts = (0 until numStores).map { i =>
            val storeId = DataStoreID(poolUUID, i.asInstanceOf[Byte])
            system.getStorageHost(storeId)
          }
          
          Future.sequence(fhosts).map { lst =>
          
          new BaseStoragePool(system, poolDefinitionPointer, kvos.revision, kvos.refcount, 
              poolUUID, lst.toArray, allocationTreeRootManager, mus)
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
    val allocationTreeRootManager: TieredKeyValueListMutableRootManager,
    val missedUpdateStrategy: MissedUpdateStrategy) extends StoragePool {
  
  import BaseStoragePool._
  
  override def supportsIDA(ida: IDA): Boolean = ida.width >= numberOfStores
  
  override def selectStoresForAllocation(ida: IDA): Array[Int] = {
    if (!supportsIDA(ida))
      throw UnsupportedIDA(uuid, ida)
    
    val availableIndicies = (0 until numberOfStores).foldLeft(List[Int]())((lst, idx) => if (storageHosts(idx).online) idx :: lst else lst)
    val randomizedIndices = scala.util.Random.shuffle(availableIndicies)
    val arr = randomizedIndices.take(ida.width).sorted.toArray
    if (arr.length < ida.width)
      throw InsufficientOnlineNodes(ida.width, arr.length)
    arr
  }
  
  def refresh()(implicit ec: ExecutionContext): Future[StoragePool] = Factory.createStoragePool(system, poolDefinitionPointer)
  
  override def getAllocationTree(retryStrategy: RetryStrategy)(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList] = {
    Future.successful(new MutableTieredKeyValueList(allocationTreeRootManager))
  }
  
  override def getAllocatedObjectsIterator()(implicit ec: ExecutionContext): Future[AllocatedObjectsIterator] = {
    getAllocationTree(system.retryStrategy).map { tree => 
      new AllocatedObjectsIterator {
        import AllocatedObjectsIterator._
        
        val iter = new TieredKeyValueListIterator(system, tree)
        
        def fetchNext()(implicit ec: ExecutionContext): Future[Option[AllocatedObject]] = iter.next().map {
          case None => None
          case Some(v) => Some(AllocatedObject(ObjectPointer(v.value), v.timestamp))
        }
      }
    }
  }
  
  def getMissedUpdateStrategy(): MissedUpdateStrategy = missedUpdateStrategy
  
  def createMissedUpdateHandler(
      transactionUUID: UUID,
      pointer: ObjectPointer, 
      missedStores: List[Byte])(implicit ec: ExecutionContext): MissedUpdateHandler = {
    system.createMissedUpdateHandler(missedUpdateStrategy, transactionUUID, pointer, missedStores)
  }

  def createMissedUpdateIterator(poolIndex: Byte)(implicit ec: ExecutionContext): MissedUpdateIterator = {
    system.createMissedUpdateIterator(missedUpdateStrategy, DataStoreID(uuid, poolIndex))
  }
} // end BaseStoragePool

