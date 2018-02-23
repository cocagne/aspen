package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.base.kvtree.KVTree
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.network.StorageNodeID
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.base.kvlist.KVListCodec
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.data_store.DataStore
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.core.objects.KeyValueObjectPointer

object Bootstrap {
  val ZeroedUUID                      = new UUID(0, 0)
  val BootstrapObjectAllocaterUUID    = ZeroedUUID
  val SystemAllocationPolicyUUID      = ZeroedUUID // to be removed with KVTree
  val BootstrapStoragePoolUUID        = ZeroedUUID
  val BootstrapTransactionUUID        = ZeroedUUID
  val StoragePoolTreeUUID             = ZeroedUUID
  val TaskGroupTreeUUID               = new UUID(0, 1)
  val SystemTreeNodeSizeLimit         = 64 * 1024
  val SystemTreeKeyComparisonStrategy = KVTree.KeyComparison.Raw
  
  val BootstrapAllocatedObjectCount   = 8
  
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
  def XXinitializeNewSystem(
      bootstrapStores: List[DataStore],
      bootstrapPoolIDA: IDA)(implicit ec: ExecutionContext): Future[DataObjectPointer] = {
    
    require( bootstrapPoolIDA.width >= bootstrapStores.length)

    val timestamp = HLCTimestamp.now
    val hosts = bootstrapStores.take(bootstrapPoolIDA.width).zipWithIndex
    val hostsArray = bootstrapStores.take(bootstrapPoolIDA.width).toArray
    
    val objectSize = bootstrapStores.foldLeft(None:Option[Int])((ox,y) => (ox, y.maximumAllowedObjectSize) match {
      case (None, None) => None
      case (Some(maxSize), None) => Some(maxSize)
      case (None, Some(maxSize)) => Some(maxSize)
      case (Some(cur), Some(nxt)) => if (cur <= nxt) Some(cur) else Some(nxt)
    })
    
    def allocateDataObject(initialContent: DataBuffer): Future[DataObjectPointer] = {
      val objectUUID = UUID.randomUUID()
      val enc = bootstrapPoolIDA.encode(initialContent)
      val storePointers = new Array[StorePointer](bootstrapPoolIDA.width)
      val falloc = hosts.map { t => {
        val (store, storeIndex) = t
        store.bootstrapAllocateNewObject(objectUUID, enc(storeIndex), timestamp).map(sp => storePointers(storeIndex) = sp) 
      }}
      
      Future.sequence(falloc) map { _ => 
          new DataObjectPointer(objectUUID, BootstrapStoragePoolUUID, objectSize, bootstrapPoolIDA, storePointers)
      }
    }
    
    def allocateKeyValueObject(initialContent: List[(Key, Array[Byte])]): Future[KeyValueObjectPointer] = {
      val objectUUID = UUID.randomUUID()
      val inserts = initialContent.map(t => new Insert(t._1.bytes, t._2, timestamp))
      val enc = KeyValueObjectCodec.encodeUpdate(bootstrapPoolIDA, inserts)
      val storePointers = new Array[StorePointer](bootstrapPoolIDA.width)
      val falloc = hosts.map { t => {
        val (store, storeIndex) = t
        store.bootstrapAllocateNewObject(objectUUID, enc(storeIndex), timestamp).map(sp => storePointers(storeIndex) = sp) 
      }}
      
      Future.sequence(falloc) map { _ => 
          new KeyValueObjectPointer(objectUUID, BootstrapStoragePoolUUID, objectSize, bootstrapPoolIDA, storePointers)
      }
    }
    
    def overwrite(pointer: KeyValueObjectPointer, initialContent: List[(Key, Array[Byte])]): Future[Unit] = {
      val inserts = initialContent.map(t => new Insert(t._1.bytes, t._2, timestamp))
      val enc = KeyValueObjectCodec.encodeUpdate(bootstrapPoolIDA, inserts)
      Future.sequence(hosts.map(t => t._1.bootstrapOverwriteObject(pointer, enc(t._2), timestamp))).map(_=>())
    }
    
   /*    * System TieredList which contains
   *        - Storage Pool TL which contains
   *            - Bootstrap Pool Definition
   *                - Points to Allocation Tree
   *                
   *        - Object Allocater TL
   *            - Bootstrap Pool Allocater
   *            
   *        - Task Group TL
   */
    Future.failed(new Exception("TODO"))
  }
  
  
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
      bootstrapPoolIDA: IDA)(implicit ec: ExecutionContext): Future[DataObjectPointer] = {
    
    require( bootstrapPoolIDA.width >= bootstrapStores.length)

    val timestamp = HLCTimestamp.now
    val hosts = bootstrapStores.take(bootstrapPoolIDA.width).zipWithIndex
    val hostsArray = bootstrapStores.take(bootstrapPoolIDA.width).toArray
    val hostingStorageNodes = List.fill(bootstrapPoolIDA.width)(StorageNodeID(ZeroedUUID)).toArray
    
    val objectSize = bootstrapStores.foldLeft(None:Option[Int])((ox,y) => (ox, y.maximumAllowedObjectSize) match {
      case (None, None) => None
      case (Some(maxSize), None) => Some(maxSize)
      case (None, Some(maxSize)) => Some(maxSize)
      case (Some(cur), Some(nxt)) => if (cur <= nxt) Some(cur) else Some(nxt)
    })
    
    def allocate(initialContent: DataBuffer): Future[DataObjectPointer] = {
      val objectUUID = UUID.randomUUID()
      val enc = bootstrapPoolIDA.encode(initialContent)
      val storePointers = new Array[StorePointer](bootstrapPoolIDA.width)
      val falloc = hosts.map { t => {
        val (store, storeIndex) = t
        store.bootstrapAllocateNewObject(objectUUID, enc(storeIndex), timestamp).map(sp => storePointers(storeIndex) = sp) 
      }}
      
      Future.sequence(falloc) map { _ => 
          new DataObjectPointer(objectUUID, BootstrapStoragePoolUUID, objectSize, bootstrapPoolIDA, storePointers)
      }
    }
    
    def allocateKV(initialContent: List[(Key, Array[Byte])]): Future[KeyValueObjectPointer] = {
      val objectUUID = UUID.randomUUID()
      val inserts = initialContent.map(t => new Insert(t._1.bytes, t._2, timestamp))
      val enc = KeyValueObjectCodec.encodeUpdate(bootstrapPoolIDA, inserts)
      val storePointers = new Array[StorePointer](bootstrapPoolIDA.width)
      val falloc = hosts.map { t => {
        val (store, storeIndex) = t
        store.bootstrapAllocateNewObject(objectUUID, enc(storeIndex), timestamp).map(sp => storePointers(storeIndex) = sp) 
      }}
      
      Future.sequence(falloc) map { _ => 
          new KeyValueObjectPointer(objectUUID, BootstrapStoragePoolUUID, objectSize, bootstrapPoolIDA, storePointers)
      }
    }
    
    def overwriteKeyValueObject(pointer: KeyValueObjectPointer, initialContent: List[(Key, Array[Byte])]): Future[Unit] = {
      val inserts = initialContent.map(t => new Insert(t._1.bytes, t._2, timestamp))
      val enc = KeyValueObjectCodec.encodeUpdate(bootstrapPoolIDA, inserts)
      Future.sequence(hosts.map(t => t._1.bootstrapOverwriteObject(pointer, enc(t._2), timestamp))).map(_=>())
    }
    
    def overwriteDataObject(objectPointer: DataObjectPointer, newContent: Array[Byte]): Future[Unit] = {
      val encodedContent = bootstrapPoolIDA.encode(DataBuffer(newContent))
      Future.sequence(hosts.map(t => t._1.bootstrapOverwriteObject(objectPointer, encodedContent(t._2), timestamp))).map(_=>())
    }
    
    def ins(p: DataObjectPointer): (Array[Byte], Array[Byte]) = (uuid2byte(p.uuid), NetworkCodec.objectPointerToByteArray(p))
    
    def treeNode(key: UUID, ptr: DataObjectPointer) = {
      val inserts = (uuid2byte(key) -> NetworkCodec.objectPointerToByteArray(ptr))::Nil
      ByteBuffer.wrap(KVListCodec.encodeNewListContent(inserts))
    }
    
    def treeDef(tier0Pointer: DataObjectPointer) = ByteBuffer.wrap(KVTree.defineNewTreeWithInitialTier0Node(
                                                               SystemAllocationPolicyUUID, SystemTreeKeyComparisonStrategy, tier0Pointer))
                                                               
    // Gets a little tiresome to constantly type out ByteBuffer -> DataBuffer conversions
    import scala.language.implicitConversions                                                        
    implicit def bb2db(bb: ByteBuffer): DataBuffer = DataBuffer(bb)
                                                               
    for {
      allocTreeTier0Ptr <- allocate(ByteBuffer.allocate(0))
      allocTreeDefnPtr <- allocate(treeDef(allocTreeTier0Ptr))
      
      bootstrapPoolDefnPtr <- allocate(BaseCodec.encodeStoragePoolDefinition(BootstrapStoragePoolUUID, hostingStorageNodes, Some(allocTreeDefnPtr)))
                                                     
      storagePoolTreeTier0Ptr <- allocate(treeNode(BootstrapStoragePoolUUID, bootstrapPoolDefnPtr))
      storagePoolTreeDefnPtr <- allocate(treeDef(storagePoolTreeTier0Ptr))
                                                         
      systemTreeTier0Ptr <- allocate(treeNode(StoragePoolTreeUUID, storagePoolTreeDefnPtr))
      systemTreeDefnPtr <- allocate(treeDef(systemTreeTier0Ptr))
      
      radiclePtr <- allocate(ByteBuffer.wrap(BaseCodec.encode(Radicle(systemTreeDefnPtr))))
      
      allocContent = List(allocTreeTier0Ptr, allocTreeDefnPtr, bootstrapPoolDefnPtr, storagePoolTreeTier0Ptr, storagePoolTreeDefnPtr,
                          systemTreeTier0Ptr, systemTreeDefnPtr, radiclePtr) map (ins)
      
      overwriteComplete <- overwriteDataObject(allocTreeTier0Ptr, KVListCodec.encodeNewListContent(allocContent))
    } 
    yield radiclePtr
  }
  
}