package com.ibm.aspen.base.impl

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.base.impl.{codec => P}
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.impl.AllocationFinalizationAction.FAContent
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.base.tieredlist.KeyValueListPointer
import com.ibm.aspen.base.tieredlist.TieredKeyValueListSplitFA
import com.ibm.aspen.base.tieredlist.TieredKeyValueList
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.base.tieredlist.TieredKeyValueListJoinFA
import com.ibm.aspen.util.Varint

object BaseCodec {
 
  //-------------------------------------------------------------------------------------------------------------------
  // AllocationFinalizationActionContent
  //
  def encode(fac: FAContent): Array[Byte] = {
     
    val builder = new FlatBufferBuilder(1024)
    
    val storagePoolDefinitionPointerOffset = NetworkCodec.encode(builder, fac.storagePoolDefinitionPointer)
    val newObjectPointerOffset = NetworkCodec.encode(builder, fac.newNodePointer)
    
    P.AllocationFinalizationActionContent.startAllocationFinalizationActionContent(builder)
    P.AllocationFinalizationActionContent.addStoragePoolDefinitionPointer(builder, storagePoolDefinitionPointerOffset)
    P.AllocationFinalizationActionContent.addNewObjectPointer(builder, newObjectPointerOffset)

    val finalOffset = P.AllocationFinalizationActionContent.endAllocationFinalizationActionContent(builder)
    
    builder.finish(finalOffset)
    
    NetworkCodec.byteBufferToArray(builder.dataBuffer())
  }
 
  def decodeAllocationFinalizationActionContent(arr: Array[Byte]): FAContent = decode(P.AllocationFinalizationActionContent.getRootAsAllocationFinalizationActionContent(ByteBuffer.wrap(arr))) 
  
  def decode(n: P.AllocationFinalizationActionContent): FAContent = {
    val sp = NetworkCodec.decode(n.storagePoolDefinitionPointer()).asInstanceOf[KeyValueObjectPointer]
    val np = NetworkCodec.decode(n.newObjectPointer())
    FAContent(sp, np)
  }
  
  //-------------------------------------------------------------------------------------------------------------------
  // TieredKeyValueListSplitFA
  //
  /*case class Content(
      keyOrdering: KeyOrdering,
      rootManagerType: UUID,
      serializedRootManager: DataBuffer,
      targetTier: Int,
      left: KeyValueListPointer, 
      inserted: List[KeyValueListPointer])
      
      rootManagerType: com.ibm.aspen.core.network.protocol.UUID;
  serializedRootManager: [byte];
  keyComparison: com.ibm.aspen.core.network.protocol.KeyComparison;
  targetTier: int;
  left: [byte];
  inserted: [byte];
      * */
  def encodeTieredKeyValueListSplitFA(c: TieredKeyValueListSplitFA.Content): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)
    
    val insVec = {
      val sz = c.inserted.foldLeft(0)((sz, lp) => sz + lp.encodedSize)
      val arr = new Array[Byte](sz)
      val bb = ByteBuffer.wrap(arr)
      c.inserted.foreach(lp => lp.encodeInto(bb))
      arr
    }

    val smr = P.TieredKeyValueListSplitFA.createSerializedRootManagerVector(builder, c.serializedRootManager.getByteArray())
    val left = P.TieredKeyValueListSplitFA.createLeftVector(builder, c.left.toArray)
    val ins = P.TieredKeyValueListSplitFA.createInsertedVector(builder, insVec)
    
    P.TieredKeyValueListSplitFA.startTieredKeyValueListSplitFA(builder)
    P.TieredKeyValueListSplitFA.addRootManagerType(builder, NetworkCodec.encode(builder, c.rootManagerType))
    P.TieredKeyValueListSplitFA.addSerializedRootManager(builder, smr)
    P.TieredKeyValueListSplitFA.addKeyComparison(builder, NetworkCodec.encodeKeyComparison(c.keyOrdering))
    P.TieredKeyValueListSplitFA.addTargetTier(builder, c.targetTier)
    P.TieredKeyValueListSplitFA.addLeft(builder, left)
    P.TieredKeyValueListSplitFA.addInserted(builder, ins)
 
    builder.finish(P.TieredKeyValueListSplitFA.endTieredKeyValueListSplitFA(builder))
    
    NetworkCodec.byteBufferToArray(builder.dataBuffer())
  }
  def decodeTieredKeyValueListSplitFA(arr: Array[Byte]): TieredKeyValueListSplitFA.Content = {
    val o = P.TieredKeyValueListSplitFA.getRootAsTieredKeyValueListSplitFA(ByteBuffer.wrap(arr))
    
    val rootManagerType = NetworkCodec.decode(o.rootManagerType())
    val smr = new Array[Byte](o.serializedRootManagerLength())
    o.serializedRootManagerAsByteBuffer().get(smr)
    val keyOrdering = NetworkCodec.decodeKeyComparison(o.keyComparison())
    val targetTier = o.targetTier()

    val left = KeyValueListPointer(o.leftAsByteBuffer())

    var ins: List[KeyValueListPointer] = Nil
    val ibb = o.insertedAsByteBuffer()
    while (ibb.remaining != 0)
      ins = KeyValueListPointer(ibb) :: ins
    
    TieredKeyValueListSplitFA.Content(keyOrdering, rootManagerType, smr, 
        targetTier, left, ins.reverse)
  }
  
}