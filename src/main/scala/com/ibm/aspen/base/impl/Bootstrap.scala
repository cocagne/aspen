package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.base.kvtree.KVTree
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.network.StorageNodeID
import java.nio.ByteBuffer
import com.ibm.aspen.core.Util
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.base.kvlist.KVListCodec
import com.ibm.aspen.core.data_store.BootstrapDataStore
import com.ibm.aspen.core.objects.StorePointer

object Bootstrap {
  val ZeroedUUID                      = new UUID(0, 0)
  val SystemAllocationPolicyUUID      = ZeroedUUID
  val BootstrapStoragePoolUUID        = ZeroedUUID
  val BootstrapTransactionUUID        = ZeroedUUID
  val StoragePoolTreeUUID             = ZeroedUUID
  val SystemTreeNodeSizeLimit         = 64 * 1024
  val SystemTreeKeyComparisonStrategy = KVTree.KeyComparison.Raw
  
  val BootstrapAllocatedObjectCount   = 8
  
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
      allocate: (ByteBuffer) => Future[ObjectPointer],
      overwriteObject: (ObjectPointer, Array[Byte]) => Future[Unit],
      bootstrapPoolIDA: IDA)(implicit ec: ExecutionContext): Future[ObjectPointer] = {
    
    val hostingStorageNodes = List.fill(bootstrapPoolIDA.width)(StorageNodeID(ZeroedUUID)).toArray
    
    def ins(p: ObjectPointer): (Array[Byte], Array[Byte]) = (Util.uuid2byte(p.uuid), NetworkCodec.objectPointerToByteArray(p))
    
    def treeNode(key: UUID, ptr: ObjectPointer) = {
      val inserts = (Util.uuid2byte(key) -> NetworkCodec.objectPointerToByteArray(ptr))::Nil
      ByteBuffer.wrap(KVListCodec.encodeNewListContent(inserts))
    }
    
    def treeDef(tier0Pointer: ObjectPointer) = ByteBuffer.wrap(KVTree.defineNewTreeWithInitialTier0Node(
                                                               SystemAllocationPolicyUUID, SystemTreeKeyComparisonStrategy, tier0Pointer))
                                                               
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
      
      overwriteComplete <- overwriteObject(allocTreeTier0Ptr, KVListCodec.encodeNewListContent(allocContent))
    } 
    yield radiclePtr
  }
  
  def initializeNewSystem(
      bootstrapStores: List[BootstrapDataStore],
      bootstrapPoolIDA: IDA)(implicit ec: ExecutionContext): Future[ObjectPointer] = {
    
    require( bootstrapPoolIDA.width >= bootstrapStores.length)
    
    val hosts = bootstrapStores.take(bootstrapPoolIDA.width).zipWithIndex
    val hostsArray = bootstrapStores.take(bootstrapPoolIDA.width).toArray
    
    val objectSize = bootstrapStores.foldLeft(None:Option[Int])((ox,y) => (ox, y.maximumAllowedObjectSize) match {
      case (None, None) => None
      case (Some(maxSize), None) => Some(maxSize)
      case (None, Some(maxSize)) => Some(maxSize)
      case (Some(cur), Some(nxt)) => if (cur <= nxt) Some(cur) else Some(nxt)
    })
    
    def allocate(initialContent: ByteBuffer): Future[ObjectPointer] = {
      val objectUUID = UUID.randomUUID()
      val enc = bootstrapPoolIDA.encode(initialContent)
      val storePointers = new Array[StorePointer](bootstrapPoolIDA.width)
      val falloc = hosts.map { t => {
        val (store, storeIndex) = t
        store.bootstrapAllocateNewObject(objectUUID, enc(storeIndex)).map(sp => storePointers(storeIndex) = sp)
      }}
      Future.sequence(falloc) map { _ => 
          ObjectPointer(objectUUID, BootstrapStoragePoolUUID, objectSize, bootstrapPoolIDA, storePointers)
      }
    }
    
    def overwrite(objectPointer: ObjectPointer, newContent: Array[Byte]): Future[Unit] = {
      val encodedContent = bootstrapPoolIDA.encode(ByteBuffer.wrap(newContent))
      Future.sequence(hosts.map(t => t._1.bootstrapOverwriteObject(objectPointer, encodedContent(t._2)))).map(_=>())
    }
    
    initializeNewSystem(allocate, overwrite, bootstrapPoolIDA)
  }
  
}