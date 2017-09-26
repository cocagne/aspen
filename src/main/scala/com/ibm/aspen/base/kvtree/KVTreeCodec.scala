package com.ibm.aspen.base.kvtree

import com.ibm.aspen.base.kvtree.{encoding => K}
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
    
    val m = encodeInsertIntoUpperTierFinalizationActionImpl(builder, tree, targetTier, nodePointer)
        
    builder.finish(m)
    
    val db = builder.dataBuffer()
    
    val arr = new Array[Byte](db.limit - db.position)
    db.get(arr)
    
    arr
  }
  def encodeInsertIntoUpperTierFinalizationActionImpl(builder: FlatBufferBuilder, tree: KVTree, targetTier: Int, nodePointer: KVListNodePointer): Int = {
    
    val treeDescriptionObjectOffset = NetworkCodec.encode(builder, tree.treeDefinitionPointer)
    val minimumOffset = K.InsertIntoUpperTier.createMinimumVector(builder, nodePointer.minimum)
    val newNodePointerOffset = NetworkCodec.encode(builder, nodePointer.objectPointer)
    
    K.InsertIntoUpperTier.startInsertIntoUpperTier(builder)
    K.InsertIntoUpperTier.addTreeDefinitionObject(builder, treeDescriptionObjectOffset)
    K.InsertIntoUpperTier.addTargetTier(builder, targetTier)
    K.InsertIntoUpperTier.addNewNodePointer(builder, newNodePointerOffset)
    K.InsertIntoUpperTier.addMinimum(builder, minimumOffset)
    K.InsertIntoUpperTier.endInsertIntoUpperTier(builder)
  }
  
  case class InsertIntoUpperTierData(treeDefinitionPointer: ObjectPointer, targetTier: Int, nodePointer: KVListNodePointer)
  
  def decodeInsertIntoUpperTierFinalizationAction(arr: Array[Byte]): InsertIntoUpperTierData = {
    decodeInsertIntoUpperTierFinalizationAction(ByteBuffer.wrap(arr))
  }
  
  def decodeInsertIntoUpperTierFinalizationAction(bb: ByteBuffer): InsertIntoUpperTierData = {
    val m = K.InsertIntoUpperTier.getRootAsInsertIntoUpperTier(bb.asReadOnlyBuffer())
    decodeInsertIntoUpperTierFinalizationActionImpl(m)
  }
  
  def decodeInsertIntoUpperTierFinalizationActionImpl(m: K.InsertIntoUpperTier): InsertIntoUpperTierData = {
    val tdp = NetworkCodec.decode(m.treeDefinitionObject())
    val nnp = NetworkCodec.decode(m.newNodePointer())
    val min = new Array[Byte](m.minimumLength())
    m.minimumAsByteBuffer().get(min)
    InsertIntoUpperTierData(tdp, m.targetTier(), KVListNodePointer(nnp, min))
  }
  
  
  def encodeTreeDefinitionImpl(builder: FlatBufferBuilder, td: KVTreeDefinition): Int = {
    val tierPointersOffset = K.KVTreeDefinition.createTierPointersVector(builder, td.tiers.map(op => NetworkCodec.encode(builder, op)).toArray)

    K.KVTreeDefinition.startKVTreeDefinition(builder)
    K.KVTreeDefinition.addAllocationPolicyUUID(builder, NetworkCodec.encode(builder, td.allocationPolicyUUID))
    K.KVTreeDefinition.addKeyComparisonStrategy(builder, td.keyComparison.id.asInstanceOf[Byte])
    K.KVTreeDefinition.addTierPointers(builder, tierPointersOffset)
    
    val m =  K.KVTreeDefinition.endKVTreeDefinition(builder)
    m
  }
  
  def decodeTreeDefinitionImpl(m: K.KVTreeDefinition): KVTreeDefinition = {
    val allocationPolicyUUID = NetworkCodec.decode(m.allocationPolicyUUID())
    
    def tier(idx: Int, l:List[ObjectPointer]): List[ObjectPointer] = if (idx == -1) 
        l
      else 
        tier(idx-1, NetworkCodec.decode(m.tierPointers(idx)) :: l)
    
    val kc = KVTree.KeyComparison(m.keyComparisonStrategy())
        
    KVTreeDefinition(allocationPolicyUUID, kc, tier(m.tierPointersLength()-1, Nil))
  }
  
  def encodeTreeDefinition(td: KVTreeDefinition): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)
    
    val m = encodeTreeDefinitionImpl(builder, td)
        
    builder.finish(m)
    
    val db = builder.dataBuffer()
    
    val arr = new Array[Byte](db.limit - db.position)
    db.get(arr)
    
    arr
  }
  
  def decodeTreeDefinition(arr: Array[Byte]): KVTreeDefinition = decodeTreeDefinition(ByteBuffer.wrap(arr))
  
  def decodeTreeDefinition(bb: ByteBuffer): KVTreeDefinition = {
    val m = K.KVTreeDefinition.getRootAsKVTreeDefinition(bb.asReadOnlyBuffer())
    decodeTreeDefinitionImpl(m)
  }
}