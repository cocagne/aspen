package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering
import com.ibm.aspen.core.objects.keyvalue.IntegerKeyOrdering
import com.ibm.aspen.core.objects.keyvalue.LexicalKeyOrdering
import com.ibm.aspen.util.Varint
import java.nio.ByteBuffer

case class TieredKeyValueListRoot(
    topTier: Int, 
    keyOrdering: KeyOrdering, 
    rootNode: KeyValueObjectPointer,
    allocaterType: UUID,
    allocaterConfig: DataBuffer) {

  def encodedSize: Int = 3 + 16 + rootNode.encodedSize + Varint.getUnsignedIntEncodingLength(allocaterConfig.size) + allocaterConfig.size

  def encodeInto(bb: ByteBuffer): Unit = {
    val orderCode = keyOrdering match {
      case ByteArrayKeyOrdering => 0
      case IntegerKeyOrdering   => 1
      case LexicalKeyOrdering   => 2
    }
    bb.put(0.asInstanceOf[Byte]) // Placeholder for a version number
    bb.put(topTier.asInstanceOf[Byte])
    bb.put(orderCode.asInstanceOf[Byte])
    bb.putLong(allocaterType.getMostSignificantBits)
    bb.putLong(allocaterType.getLeastSignificantBits)
    rootNode.encodeInto(bb)
    Varint.putUnsignedInt(bb, allocaterConfig.size)
    bb.put(allocaterConfig.asReadOnlyBuffer())
  }
    
  def toArray: Array[Byte] = {
    val arr = new Array[Byte](encodedSize)
    val bb = ByteBuffer.wrap(arr)
    encodeInto(bb)
    arr
  }
}
  
object TieredKeyValueListRoot {

  def apply(arr: Array[Byte]): TieredKeyValueListRoot = apply(ByteBuffer.wrap(arr))
  
  def apply(bb: ByteBuffer): TieredKeyValueListRoot = {
    bb.get() // Placeholder for a version number
    
    val topTier = bb.get() 
    
    val keyOrdering = bb.get() match {
      case 0 => ByteArrayKeyOrdering
      case 1 => IntegerKeyOrdering
      case 2 =>LexicalKeyOrdering
    }
    
    val msb = bb.getLong()
    val lsb = bb.getLong()
    val allocaterType = new UUID(msb,lsb) 
      
    val rootNode = KeyValueObjectPointer(bb)
    
    val configSize = Varint.getUnsignedInt(bb)
    val allocaterConfig = new Array[Byte](configSize)
    bb.get(allocaterConfig)
    
    TieredKeyValueListRoot(topTier, keyOrdering, rootNode, allocaterType, allocaterConfig)
  }
}
  