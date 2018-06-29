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
  // TieredKeyValueListJoinFA
  //
  // NOTE: The structure of this message is identical to Split. So this method is implemented in terms of Split
  //
  def encodeTieredKeyValueListJoinFA(
      treeIdentifier: Key, treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root], keyOrdering: KeyOrdering,
      targetTier: Int, left: KeyValueListPointer, right: KeyValueListPointer): Array[Byte] = {
    encodeTieredKeyValueListSplitFA(treeIdentifier, treeContainer, keyOrdering, targetTier, left, right)
  }
  def decodeTieredKeyValueListJoinFA(arr: Array[Byte]): TieredKeyValueListJoinFA.Content = {
    val c = decodeTieredKeyValueListSplitFA(arr)
    TieredKeyValueListJoinFA.Content(c.treeIdentifier, c.treeContainer, c.targetTier, c.left, c.right, c.keyOrdering)
  }
  
  //-------------------------------------------------------------------------------------------------------------------
  // TieredKeyValueListSplitFA
  //
  def encodeTieredKeyValueListSplitFA(
      treeIdentifier: Key, treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root], keyOrdering: KeyOrdering,
      targetTier: Int, left: KeyValueListPointer, right: KeyValueListPointer): Array[Byte] = {
    val builder = new FlatBufferBuilder(1024)
    
    val treeIdentifierOffset = P.TieredKeyValueListSplitFA.createTreeIdentifierKeyVector(builder, treeIdentifier.bytes)
    val (containingObjectOffset, treeRootOffset) = treeContainer match {
      case Left(pointer) => (NetworkCodec.encode(builder, pointer), -1)
      case Right(root) => (-1, P.TieredKeyValueListSplitFA.createContainingTreeRootVector(builder, root.toArray()))
    }
    
    val leftMinimum = P.TieredKeyValueListSplitFA.createRightMinimumVector(builder, left.minimum.bytes)
    val leftPointer = NetworkCodec.encode(builder, left.pointer)
    val rightMinimum = P.TieredKeyValueListSplitFA.createRightMinimumVector(builder, right.minimum.bytes)
    val rightPointer = NetworkCodec.encode(builder, right.pointer)
    
    P.TieredKeyValueListSplitFA.startTieredKeyValueListSplitFA(builder)
    P.TieredKeyValueListSplitFA.addTreeIdentifierKey(builder, treeIdentifierOffset)
    
    if (containingObjectOffset >= 0)
      P.TieredKeyValueListSplitFA.addContainingObject(builder, containingObjectOffset)
    if (treeRootOffset >= 0)
      P.TieredKeyValueListSplitFA.addContainingTreeRoot(builder, treeRootOffset)
      
    P.TieredKeyValueListSplitFA.addKeyComparison(builder, NetworkCodec.encodeKeyComparison(keyOrdering))
    
    P.TieredKeyValueListSplitFA.addTargetTier(builder, targetTier)
    
    P.TieredKeyValueListSplitFA.addLeftMinimum(builder, leftMinimum)
    P.TieredKeyValueListSplitFA.addLeftPointer(builder, leftPointer)
    P.TieredKeyValueListSplitFA.addRightMinimum(builder, rightMinimum)
    P.TieredKeyValueListSplitFA.addRightPointer(builder, rightPointer)
    
    val finalOffset = P.TieredKeyValueListSplitFA.endTieredKeyValueListSplitFA(builder)   
 
    builder.finish(finalOffset)
    
    val db = builder.dataBuffer()
    
    val arr = new Array[Byte](db.limit - db.position)
    db.get(arr)
    
    arr
  }
  def decodeTieredKeyValueListSplitFA(arr: Array[Byte]): TieredKeyValueListSplitFA.Content = {
    val o = P.TieredKeyValueListSplitFA.getRootAsTieredKeyValueListSplitFA(ByteBuffer.wrap(arr))
    
    val karr = new Array[Byte](o.treeIdentifierKeyLength())
    o.treeIdentifierKeyAsByteBuffer().get(karr)
    val treeIdentifier = Key(karr)
    
    val larr = new Array[Byte](o.leftMinimumLength())
    o.leftMinimumAsByteBuffer().get(larr)
    val leftMinimum = Key(larr)
    
    val leftPointer = NetworkCodec.decode(o.leftPointer()).asInstanceOf[KeyValueObjectPointer]
    
    val rarr = new Array[Byte](o.rightMinimumLength())
    o.rightMinimumAsByteBuffer().get(rarr)
    val rightMinimum = Key(rarr)
    
    val rightPointer = NetworkCodec.decode(o.rightPointer()).asInstanceOf[KeyValueObjectPointer]
    
    val treeContainer: Either[KeyValueObjectPointer, TieredKeyValueList.Root] = if (o.containingTreeRootLength() > 0)
        Right(TieredKeyValueList.Root(o.containingTreeRootAsByteBuffer()))
      else
        Left(NetworkCodec.decode(o.containingObject()).asInstanceOf[KeyValueObjectPointer])
    
    val keyOrdering = NetworkCodec.decodeKeyComparison(o.keyComparison())
    
    TieredKeyValueListSplitFA.Content(treeIdentifier, treeContainer, o.targetTier(),
        KeyValueListPointer(leftMinimum, leftPointer), KeyValueListPointer(rightMinimum, rightPointer), keyOrdering)
  }
  
  
  
}