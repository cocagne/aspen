package com.ibm.aspen.base.kvtree

import com.ibm.aspen.base.kvtree.{encoding => K}
import com.ibm.aspen.core.network.NetworkCodec
import com.google.flatbuffers.FlatBufferBuilder
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer
import scala.collection.immutable.SortedMap
import com.ibm.aspen.base.kvlist.KVListNodePointer
import com.ibm.aspen.base.kvlist.KVListCodec
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.DataObjectPointer

private[kvtree] object KVTreeCodec {
  
  def encode(ic: List[KVListNodePointer], rptr: Option[KVListNodePointer]): DataBuffer = {
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
  
  case class InsertIntoUpperTierData(treeDefinitionPointer: DataObjectPointer, targetTier: Int, nodePointer: KVListNodePointer)
  
  def decodeInsertIntoUpperTierFinalizationAction(arr: Array[Byte]): InsertIntoUpperTierData = {
    decodeInsertIntoUpperTierFinalizationAction(DataBuffer(arr))
  }
  
  def decodeInsertIntoUpperTierFinalizationAction(bb: DataBuffer): InsertIntoUpperTierData = {
    val m = K.InsertIntoUpperTier.getRootAsInsertIntoUpperTier(bb.asReadOnlyBuffer())
    decodeInsertIntoUpperTierFinalizationActionImpl(m)
  }
  
  def decodeInsertIntoUpperTierFinalizationActionImpl(m: K.InsertIntoUpperTier): InsertIntoUpperTierData = {
    val tdp = NetworkCodec.decode(m.treeDefinitionObject()).asInstanceOf[DataObjectPointer]
    val nnp = NetworkCodec.decode(m.newNodePointer())
    val min = new Array[Byte](m.minimumLength())
    m.minimumAsByteBuffer().get(min)
    nnp match {
      case d: DataObjectPointer => InsertIntoUpperTierData(tdp, m.targetTier(), KVListNodePointer(d, min))
      case _ => throw new Exception("Unsupported Pointer Type")
    }
    
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
    
    def tier(idx: Int, l:List[DataObjectPointer]): List[DataObjectPointer] = if (idx == -1) 
        l
      else 
        tier(idx-1, NetworkCodec.decode(m.tierPointers(idx)).asInstanceOf[DataObjectPointer] :: l)
    
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
  
  def decodeTreeDefinition(arr: Array[Byte]): KVTreeDefinition = decodeTreeDefinition(DataBuffer(arr))
  
  def decodeTreeDefinition(bb: DataBuffer): KVTreeDefinition = {
    val m = K.KVTreeDefinition.getRootAsKVTreeDefinition(bb.asReadOnlyBuffer())
    decodeTreeDefinitionImpl(m)
  }
}