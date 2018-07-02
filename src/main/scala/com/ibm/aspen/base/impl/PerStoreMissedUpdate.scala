package com.ibm.aspen.base.impl

import java.util.UUID
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.base.tieredlist.TieredKeyValueList
import com.ibm.aspen.base.tieredlist.SimpleMutableTieredKeyValueList
import com.ibm.aspen.core.read.CorruptedObject
import com.ibm.aspen.base.StopRetrying
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering
import com.ibm.aspen.base.TypeFactory
import com.ibm.aspen.base.MissedUpdateStrategy
import com.ibm.aspen.base.StoragePool
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.base.MissedUpdateHandlerFactory
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.base.MissedUpdateHandler
import scala.concurrent.Promise
import com.ibm.aspen.base.MissedUpdateIterator
import com.ibm.aspen.util.byte2uuid
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.transaction.KeyValueUpdate

/**
 *  Strategy: Store in the pool definition a MutableTieredKeyValueList for each store that contains the UUIDs of
 *  all objects peer nodes thing the store may have missed. Concurrent modification performance should be decent
 *  due to blind appends and multiple updates to the same object will be compacted down to a single entry during
 *  pre-split compaction.
 *  
 */
object PerStoreMissedUpdate extends MissedUpdateHandlerFactory {
 
  val typeUUID = UUID.fromString("fed25913-19e0-4045-b45c-2fc30d3200f1")
  
  def getStrategy(objectAllocaters: Array[UUID], tierNodeSizes: Array[Int]): MissedUpdateStrategy = {
    MissedUpdateStrategy(typeUUID, Some(encodeTreeConfig(objectAllocaters, tierNodeSizes)))
  }
  
  def encodeTreeConfig(objectAllocaters: Array[UUID], tierNodeSizes: Array[Int]): Array[Byte] = {
    val arr = new Array[Byte](1 + 1 + 16 * objectAllocaters.length + 4 * tierNodeSizes.length)
    val bb = ByteBuffer.wrap(arr)
    bb.put(objectAllocaters.length.asInstanceOf[Byte])
    bb.put(tierNodeSizes.length.asInstanceOf[Byte])
    objectAllocaters.foreach { u =>
      bb.putLong(u.getMostSignificantBits)
      bb.putLong(u.getLeastSignificantBits)
    }
    tierNodeSizes.foreach( i => bb.putInt(i) )
    arr
  }
  
  def decodeTreeConfig(arr: Array[Byte]): (Array[UUID], Array[Int]) = {
    val bb = ByteBuffer.wrap(arr)
    val nu = bb.get()
    val ni = bb.get()
    val objectAllocaters = (0 until nu).map { _ =>
      val msb = bb.getLong()
      val lsb = bb.getLong()
      new UUID(msb, lsb)
    }.toArray
    val tierNodeSizes = (0 until ni).map( _ => bb.getInt() ).toArray
    (objectAllocaters, tierNodeSizes)
  }
  
  def storeKey(storeIndex: Byte): Key = {
    // prefix with UUID to prevent key clashes
    val arr = new Array[Byte](17)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(typeUUID.getMostSignificantBits)
    bb.putLong(typeUUID.getLeastSignificantBits)
    bb.put(storeIndex)
    Key(arr)
  }
  
  def loadMissedUpdateTree(
      system: AspenSystem, 
      poolUUID: UUID, 
      storeIndex: Byte)(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList] = {
    
    val treeKey = storeKey(storeIndex)
    
    // Fail if the pool object has been deleted
    def onAttemptFailure(t: Throwable): Future[Unit] = t match {
      case t: CorruptedObject => throw new StopRetrying(t)
      case t => Future.unit
    }
    
    def getOrCreate(pool: StoragePool, kvos: KeyValueObjectState): Future[SimpleMutableTieredKeyValueList] = kvos.contents.get(treeKey) match {
      case Some(v) => Future.successful(SimpleMutableTieredKeyValueList.load(system, pool.poolDefinitionPointer, treeKey, v.value))
      
      case None =>
        val (objectAllocaters, tierNodeSizes) = decodeTreeConfig(pool.getMissedUpdateStrategy().config.get)
        
        SimpleMutableTieredKeyValueList.create(system, kvos, treeKey, objectAllocaters, tierNodeSizes, ByteArrayKeyOrdering)
    }
    
    // Race condition between multiple peers simultaneously attempting to create the tree could conflict and
    // cause failures. Continually re-read and re-attempt until either creation succeeds or we see that someone
    // else created it
    system.retryStrategy.retryUntilSuccessful(onAttemptFailure _) {
      for {
        pool <- system.getStoragePool(poolUUID)
        kvos <- system.readObject(pool.poolDefinitionPointer)
        t <- getOrCreate(pool, kvos)
      } yield t
    }
  }
  
  class MUIterator(
      val system: AspenSystem, 
      val storeId: DataStoreID)(implicit ec: ExecutionContext) extends MissedUpdateIterator {
    
    private[this] var uuids: List[(UUID, HLCTimestamp)] = Nil
    private[this] var fnode = loadMissedUpdateTree(system, storeId.poolUUID, storeId.poolIndex).flatMap(_.fetchMutableNode(Key.AbsoluteMinimum))
    
    val ftree = loadMissedUpdateTree(system, storeId.poolUUID, storeId.poolIndex)
    
    def objectUUID: Option[UUID] = synchronized { 
      if (uuids.isEmpty) None else Some(uuids.head._1) 
    }
    
    def fetchNext()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
      if (uuids.isEmpty) {
        fnode = loadMissedUpdateTree(system, storeId.poolUUID, storeId.poolIndex).flatMap(_.fetchMutableNode(Key.AbsoluteMinimum))
        fnode map { node => synchronized {
          uuids = node.kvos.contents.valuesIterator.map(v => (byte2uuid(v.key.bytes), v.timestamp)).toList
        }}
      } else {
        uuids = uuids.tail
        Future.unit
      }
    }
    
    def markRepaired()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
      if (uuids.isEmpty)
        Future.unit
      else {
        system.transact { implicit tx =>
          for {
            node <- fnode
            key = Key(uuids.head._1)
            reqs = List(KeyValueUpdate.KVRequirement(key, uuids.head._2, KeyValueUpdate.TimestampRequirement.Equals))
            _ <- node.prepreUpdateTransaction(Nil, List(key), reqs)
          } yield()
        }
      }
    }
  }
  
  def markMissedObject(
      system: AspenSystem, 
      obj: ObjectPointer, 
      storeIndex: Byte)(implicit ec: ExecutionContext): Future[Unit] = {
    
    println(s"**** MARKING MISSED UPDATE **** ${obj.uuid} for store ${storeIndex}")
    
    // Fail if the pool object has been deleted
    def onAttemptFailure(t: Throwable): Future[Unit] = t match {
      case t: CorruptedObject => throw new StopRetrying(t)
    }
    
    val objKey = Key(obj.uuid)
    
    system.transactUntilSuccessfulWithRecovery(onAttemptFailure _) { implicit tx =>
      
      // Prevent potentially infinite recursion
      tx.disableMissedUpdateTracking()
      
      for {
        tl <- loadMissedUpdateTree(system, obj.poolUUID, storeIndex)
        node <- tl.fetchMutableNode(objKey)
        prep <- node.prepreUpdateTransaction(List((objKey -> new Array[Byte](0))), Nil, Nil)
      } yield ()
    }
  }
  
  def createHandler(
      mus: MissedUpdateStrategy, 
      system: AspenSystem,
      pointer: ObjectPointer, 
      missedStores: List[Byte])(implicit ec: ExecutionContext): MissedUpdateHandler = {
    return new MissedUpdateHandler {
      
      def execute(): Future[Unit] = {
        Future.sequence( missedStores.map(storeIdx => markMissedObject(system, pointer, storeIdx)) ).map(_=>())
      }
    }
  }
  
  def createIterator(
      mus: MissedUpdateStrategy,
      system: AspenSystem,
      storeId: DataStoreID)(implicit ec: ExecutionContext): MissedUpdateIterator = new MUIterator(system, storeId)
  
}

