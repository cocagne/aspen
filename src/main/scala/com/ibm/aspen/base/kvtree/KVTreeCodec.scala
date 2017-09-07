package com.ibm.aspen.base.kvtree

import com.ibm.aspen.base.{kvtree => K}
import com.ibm.aspen.core.network.{Codec => NetworkCodec}
import com.google.flatbuffers.FlatBufferBuilder
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer
import scala.collection.immutable.SortedMap
import com.ibm.aspen.base.kvlist.KVListNodePointer

private[kvtree] object KVTreeCodec {
  
  def encodeTreeDescription(allocationPolicyUUID: UUID, tiers: List[ObjectPointer]): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)
    
    val tierPointersOffset = K.KVTreeDescription.createTierPointersVector(builder, tiers.map(op => NetworkCodec.encode(builder, op)).toArray)

    K.KVTreeDescription.startKVTreeDescription(builder)
    K.KVTreeDescription.addAllocationPolicyUUID(builder, NetworkCodec.encode(builder, allocationPolicyUUID))
    K.KVTreeDescription.addTierPointers(builder, tierPointersOffset)
    
    val m =  K.KVTreeDescription.endKVTreeDescription(builder)
        
    builder.finish(m)
    
    val db = builder.dataBuffer()
    
    val arr = new Array[Byte](db.capacity - db.position)
    db.get(arr)
    
    arr
  }
  
  def decodeTreeDescription(bb: ByteBuffer): (UUID, List[ObjectPointer]) = {
    val m = K.KVTreeDescription.getRootAsKVTreeDescription(bb.asReadOnlyBuffer())
    val allocationPolicyUUID = NetworkCodec.decode(m.allocationPolicyUUID())
    
    def tier(idx: Int, l:List[ObjectPointer]): List[ObjectPointer] = if (idx == -1) 
        l
      else 
        tier(idx-1, NetworkCodec.decode(m.tierPointers(idx)) :: l)
    
    (allocationPolicyUUID, tier(m.tierPointersLength()-1, Nil))
  }
  
}