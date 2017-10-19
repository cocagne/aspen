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

object Bootstrap {
  val ZeroedUUID                      = new UUID(0, 0)
  val SystemAllocationPolicyUUID      = ZeroedUUID
  val BootstrapStoragePoolUUID        = ZeroedUUID
  val StoragePoolTreeUUID             = ZeroedUUID
  val SystemTreeNodeSizeLimit         = 64 * 1024
  val SystemTreeKeyComparisonStrategy = KVTree.KeyComparison.Raw
  
  /** Creates the Radicle object and the minimal set of supporting data structures. Returns a Future to the Radicle ObjectPointer
   * 
   * Steps:
   *   - Allocate the bootstrap allocation tree tier0 object
   *   - Allocate the bootstrap pool tree definition object (points to tier0 object)
   *   - Allocate the storage pool definition object for bootstrap pool (points to bootstrap pool tree def object)
   *   - Allocate the system tree tree definition object
   *   - Allocate the Radicle (points to system tree tree def object and storage pool def object)
   *   - Update tier0 allocation object to include all allocated object pointers
   * 
   */
  def initializeNewSystem(
      allocate: (ByteBuffer) => Future[ObjectPointer],
      overwriteObject: (ObjectPointer, Array[Byte]) => Future[Unit],
      bootstrapPoolIDA: IDA)(implicit ec: ExecutionContext): Future[ObjectPointer] = {
    
    val hostingStorageNodes = List.fill(bootstrapPoolIDA.width)(StorageNodeID(ZeroedUUID)).toArray
    
    def ins(p: ObjectPointer): (Array[Byte], Array[Byte]) = (Util.uuid2byte(p.uuid), NetworkCodec.objectPointerToByteArray(p))
  
    for {
      tier0AllocTreePtr <- allocate(ByteBuffer.allocate(0))
      allocTreeDefnPtr <- allocate(ByteBuffer.wrap(KVTree.defineNewTreeWithInitialTier0Node(
                                                     SystemAllocationPolicyUUID, SystemTreeKeyComparisonStrategy, tier0AllocTreePtr)))
      bootstrapPoolDefnPtr <- allocate(BaseCodec.encode(BootstrapStoragePoolUUID, hostingStorageNodes, Some(allocTreeDefnPtr)))
      systemTreeDefnPtr <- allocate(ByteBuffer.wrap(KVTree.defineNewTree(SystemAllocationPolicyUUID, SystemTreeKeyComparisonStrategy)))
      radiclePtr <- allocate(ByteBuffer.wrap(BaseCodec.encode(Radicle(bootstrapPoolDefnPtr, systemTreeDefnPtr))))
      
      allocContent = List(tier0AllocTreePtr, allocTreeDefnPtr, bootstrapPoolDefnPtr, systemTreeDefnPtr, radiclePtr) map (ins)
      
      overwriteComplete <- overwriteObject(tier0AllocTreePtr, KVListCodec.encodeNewListContent(allocContent))
    } 
    yield radiclePtr
  }
}