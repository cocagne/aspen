package com.ibm.aspen.base.impl

import java.util.UUID

import com.ibm.aspen.base.MissedUpdateStrategy
import com.ibm.aspen.base.tieredlist.{SimpleTieredKeyValueListNodeAllocater, TieredKeyValueListRoot}
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.{DataStore, StoreKeyValueObjectContent}
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, ObjectRevision, StorePointer}
import com.ibm.aspen.core.objects.keyvalue.{ByteArrayKeyOrdering, Insert, Key, KeyValueOperation}

import scala.concurrent.{ExecutionContext, Future}

object Bootstrap {
  val ZeroedUUID                      = new UUID(0, 0)
 
  val BootstrapObjectAllocaterUUID    = ZeroedUUID
  val SystemTreeKey                   = Key(Array[Byte](0))
  
  val StoragePoolTreeUUID             = new UUID(0, 1)
  val TaskGroupTreeUUID               = new UUID(0, 2)
  val ObjectAllocaterTreeUUID         = new UUID(0, 3)
  
  val BootstrapStoragePoolUUID        = ZeroedUUID
  
  val SystemTreeNodeSizeLimit         = 64 * 1024
  val SystemTreeKVPairLimit           = 20
  
  
  val BootstrapAllocatedObjectCount   = 6
  
  
  
  import com.ibm.aspen.util.uuid2byte
  
  /** Creates the Radicle object and the minimal set of supporting data structures. Returns a Future to the Radicle ObjectPointer
   *  
   *  Radicle is a KeyValueObject that contains
   *  
   *    * System TieredList which contains
   *        - Storage Pool TL which contains
   *            - Bootstrap Pool Definition
   *                - Points to Allocation Tree
   *                
   *        - Object Allocater TL
   *            - Bootstrap Pool Allocater
   *            
   *        - Task Group TL
   */
  
  /** Creates the Radicle object and the minimal set of supporting data structures. Returns a Future to the Radicle ObjectPointer
   * Add StoragePoolTreeDef + tier0
   * Add system tree tier0 w/ pointer to StoragePoolTreeDef
   * Steps:
   *   - Allocate the bootstrap pool allocation tree tier0 object
   *   - Allocate the bootstrap pool allocation tree definition object (points to tier0 object)
   *   
   *   - Allocate the storage pool definition object for bootstrap pool (points to bootstrap pool allocation tree defn object)
   *   
   *   - Allocate the storage pool tree tier0 object (points to storage pool defn object)
   *   - Allocate the storage pool tree definition object (points to tier0 object)
   *   
   *   - Allocate system tree tier0 object (points to storage pool tree defn)
   *   - Allocate the system tree definition object (points to tier0 object)
   *   
   *   - Allocate the Radicle (points to system tree def object)
   *   
   *   - Update tier0 allocation object to include all allocated object pointers
   * 
   */
  def initializeNewSystem(
      bootstrapStores: List[DataStore],
      bootstrapPoolIDA: IDA,
      bootstrapPoolMissedUpdateStrategy: MissedUpdateStrategy)(implicit ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    
    require( bootstrapPoolIDA.width >= bootstrapStores.length)

    val txRevision = ObjectRevision(new UUID(0,0))
    val timestamp = HLCTimestamp.now
    val hosts = bootstrapStores.take(bootstrapPoolIDA.width).zipWithIndex
    //val hostsArray = bootstrapStores.take(bootstrapPoolIDA.width).toArray
    
    val objectSize = bootstrapStores.foldLeft(None:Option[Int])((ox,y) => (ox, y.maximumAllowedObjectSize) match {
      case (None, None) => None
      case (Some(maxSize), None) => Some(maxSize)
      case (None, Some(maxSize)) => Some(maxSize)
      case (Some(cur), Some(nxt)) => if (cur <= nxt) Some(cur) else Some(nxt)
    })
    
    def allocateKV(initialContent: List[(Key, Array[Byte])]): Future[KeyValueObjectPointer] = {
      val objectUUID = UUID.randomUUID()
      val inserts = initialContent.map(t => new Insert(t._1, t._2))
      val enc = KeyValueOperation.encode(inserts, bootstrapPoolIDA)
      val storePointers = new Array[StorePointer](bootstrapPoolIDA.width)
      val falloc = hosts.map { t => {
        val (store, storeIndex) = t
        store.bootstrapAllocateNewObject(objectUUID, 
            StoreKeyValueObjectContent().update(enc(storeIndex), txRevision, timestamp).encode(),
            timestamp).map(sp => storePointers(storeIndex) = sp) 
      }}
      Future.sequence(falloc) map { _ =>
          new KeyValueObjectPointer(objectUUID, BootstrapStoragePoolUUID, objectSize, bootstrapPoolIDA, storePointers)
      }
    }
    
    def overwriteKeyValueObject(pointer: KeyValueObjectPointer, initialContent: List[(Key, Array[Byte])]): Future[Unit] = {
      val inserts = initialContent.map(t => new Insert(t._1, t._2))
      val enc = KeyValueOperation.encode(inserts, bootstrapPoolIDA)
      Future.sequence(hosts.map(t => t._1.bootstrapOverwriteObject(pointer, StoreKeyValueObjectContent().update(enc(t._2), txRevision, timestamp).encode(), timestamp))).map(_=>())
    }
    
    def updateAllocationTree(allocTreeRoot: KeyValueObjectPointer, pointers: List[KeyValueObjectPointer]) : Future[Unit] = {
      overwriteKeyValueObject(allocTreeRoot, pointers.map(p => (Key(p.uuid), p.toArray)) )
    }
    
    def treeRoot(rootNode: KeyValueObjectPointer): Array[Byte] = {
      val allocaterType = SimpleTieredKeyValueListNodeAllocater.typeUUID
      val allocaterConfig = SimpleTieredKeyValueListNodeAllocater.encode(Array(BootstrapObjectAllocaterUUID), Array(SystemTreeNodeSizeLimit), Array(SystemTreeKVPairLimit))
      TieredKeyValueListRoot(0, ByteArrayKeyOrdering, rootNode, allocaterType, allocaterConfig).toArray
    }
    
    def getPoolKV(allocPtr: KeyValueObjectPointer): List[(Key, Array[Byte])] = {
      val ipoolkv = List(
            (BaseStoragePool.PoolUUIDKey,                 uuid2byte(BootstrapStoragePoolUUID)), 
            (BaseStoragePool.NumberOfStoresKey,           BaseStoragePool.encodeNumberOfStores(bootstrapPoolIDA.width)),
            (BaseStoragePool.AllocationTreeKey,           treeRoot(allocPtr)),
            (BaseStoragePool.MissedUpdateStrategyUUIDKey, uuid2byte(bootstrapPoolMissedUpdateStrategy.strategyUUID))
        )
      bootstrapPoolMissedUpdateStrategy.config match {
        case None => ipoolkv
        case Some(v) => (BaseStoragePool.MissedUpdateStrategyCfgKey, v) :: ipoolkv
      }
    }
    
    for {
      allocPtr <- allocateKV(Nil)
      
      bootstrapPoolPtr <- allocateKV(getPoolKV(allocPtr))

      storagePoolTreePtr <- allocateKV(List( (Key(BootstrapStoragePoolUUID), bootstrapPoolPtr.toArray) ))

      taskGroupTreePtr <- allocateKV(Nil)
      
      systemTreePtr <- allocateKV(List( 
          (Key(StoragePoolTreeUUID), treeRoot(storagePoolTreePtr)) ,
          (Key(TaskGroupTreeUUID), treeRoot(taskGroupTreePtr))
      ))
      
      radiclePtr <- allocateKV(List( 
          (SystemTreeKey, treeRoot(systemTreePtr)) 
      ))
      
      _ <- updateAllocationTree(allocPtr, List(
          allocPtr,
          bootstrapPoolPtr,
          storagePoolTreePtr,
          taskGroupTreePtr,
          systemTreePtr,
          radiclePtr
      ))
    } 
    yield radiclePtr
  }
  
}