package com.ibm.aspen.base.kvtree

import com.ibm.aspen.base.{kvtree => K}
import com.ibm.aspen.core.network.{Codec => NetworkCodec}
import com.google.flatbuffers.FlatBufferBuilder
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer
import scala.collection.immutable.SortedMap
import com.ibm.aspen.base.kvlist.KVListNodePointer
import com.ibm.aspen.base.kvlist.KVListCodec

private[kvtree] object KVTreeCodec {
  
  def encode(ic: List[KVListNodePointer], rptr: Option[KVListNodePointer]): ByteBuffer = {
    val iops = ic.map(np => KVListCodec.Insert(np.minimum, NetworkCodec.objectPointerToByteArray(np.objectPointer)))
    val fullOps = rptr match {
      case None => iops
      case Some(rp) => KVListCodec.SetRightPointer(rp.objectPointer, rp.minimum) :: iops
    }
    KVListCodec.encode(fullOps)
  }
  
  def encodeInsertIntoUpperTierFinalizationAction(tree: KVTree, targetTier: Int, nodePointer: KVListNodePointer): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)
    
    val m = encodeInsertIntoUpperTierFinalizationActionImpl(tree, targetTier, nodePointer)
        
    builder.finish(m)
    
    val db = builder.dataBuffer()
    
    val arr = new Array[Byte](db.capacity - db.position)
    db.get(arr)
    
    arr
  }
  def encodeInsertIntoUpperTierFinalizationActionImpl(tree: KVTree, targetTier: Int, nodePointer: KVListNodePointer): Int = {
    val builder = new FlatBufferBuilder(4096)
    val treeDescriptionObjectOffset = NetworkCodec.encode(builder, tree.treeDescriptionPointer)
    val minimumOffset = K.InsertIntoUpperTier.createMinimumVector(builder, nodePointer.minimum)
    val newNodePointerOffset = NetworkCodec.encode(builder, nodePointer.objectPointer)
    
    K.InsertIntoUpperTier.startInsertIntoUpperTier(builder)
    K.InsertIntoUpperTier.addTreeDescriptionObject(builder, treeDescriptionObjectOffset)
    K.InsertIntoUpperTier.addTargetTier(builder, targetTier)
    K.InsertIntoUpperTier.addNewNodePointer(builder, newNodePointerOffset)
    K.InsertIntoUpperTier.addMinimum(builder, minimumOffset)
    K.InsertIntoUpperTier.endInsertIntoUpperTier(builder)
  }
  
  case class InsertIntoUpperTierData(treeDescriptionPointer: ObjectPointer, targetTier: Int, nodePointer: KVListNodePointer)
  
   def decodeInsertIntoUpperTierFinalizationAction(arr: Array[Byte]): InsertIntoUpperTierData = decodeInsertIntoUpperTierFinalizationAction(ByteBuffer.wrap(arr))
  
  def decodeInsertIntoUpperTierFinalizationAction(bb: ByteBuffer): InsertIntoUpperTierData = {
    val m = K.InsertIntoUpperTier.getRootAsInsertIntoUpperTier(bb.asReadOnlyBuffer())
    decodeInsertIntoUpperTierFinalizationActionImpl(m)
  }
  
  def decodeInsertIntoUpperTierFinalizationActionImpl(m: K.InsertIntoUpperTier): InsertIntoUpperTierData = {
    val tdp = NetworkCodec.decode(m.treeDescriptionObject())
    val nnp = NetworkCodec.decode(m.newNodePointer())
    val min = new Array[Byte](m.minimumLength())
    m.minimumAsByteBuffer().get(min)
    InsertIntoUpperTierData(tdp, m.targetTier(), KVListNodePointer(nnp, min))
  }
  
  
  def encodeTreeDescriptionImpl(builder: FlatBufferBuilder, allocationPolicyUUID: UUID, tiers: List[ObjectPointer]): Int = {
    val tierPointersOffset = K.KVTreeDescription.createTierPointersVector(builder, tiers.map(op => NetworkCodec.encode(builder, op)).toArray)

    K.KVTreeDescription.startKVTreeDescription(builder)
    K.KVTreeDescription.addAllocationPolicyUUID(builder, NetworkCodec.encode(builder, allocationPolicyUUID))
    K.KVTreeDescription.addTierPointers(builder, tierPointersOffset)
    
    val m =  K.KVTreeDescription.endKVTreeDescription(builder)
    m
  }
  
  def decodeTreeDescriptionImpl(m: K.KVTreeDescription): (UUID, List[ObjectPointer]) = {
    val allocationPolicyUUID = NetworkCodec.decode(m.allocationPolicyUUID())
    
    def tier(idx: Int, l:List[ObjectPointer]): List[ObjectPointer] = if (idx == -1) 
        l
      else 
        tier(idx-1, NetworkCodec.decode(m.tierPointers(idx)) :: l)
    
    (allocationPolicyUUID, tier(m.tierPointersLength()-1, Nil))
  }
  
  def encodeTreeDescription(allocationPolicyUUID: UUID, tiers: List[ObjectPointer]): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)
    
    val m = encodeTreeDescriptionImpl(builder, allocationPolicyUUID, tiers)
        
    builder.finish(m)
    
    val db = builder.dataBuffer()
    
    val arr = new Array[Byte](db.capacity - db.position)
    db.get(arr)
    
    arr
  }
  
  def decodeTreeDescription(arr: Array[Byte]): (UUID, List[ObjectPointer]) = decodeTreeDescription(ByteBuffer.wrap(arr))
  
  def decodeTreeDescription(bb: ByteBuffer): (UUID, List[ObjectPointer]) = {
    val m = K.KVTreeDescription.getRootAsKVTreeDescription(bb.asReadOnlyBuffer())
    decodeTreeDescriptionImpl(m)
  }
}