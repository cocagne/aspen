package com.ibm.aspen.base.keyvalue

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.util.Varint

final case class KeyValueListPointer(minimum:Key, pointer:KeyValueObjectPointer) {
  
  import KeyValueListPointer._
  
  def toArray: Array[Byte] = encodeToByteArray(this)
  
  def encodedSize: Int = {
    val ptrSize = pointer.encodedSize
    
    Varint.getUnsignedIntEncodingLength(minimum.bytes.length) + 
    Varint.getUnsignedIntEncodingLength(ptrSize) +
    minimum.bytes.length +
    ptrSize
  }
}

object KeyValueListPointer {
  val AbsoluteMinimum = Key(new Array[Byte](0))
  
  def encodeToByteArray(p: KeyValueListPointer): Array[Byte] = {
    val arr = new Array[Byte](p.encodedSize)
    val bb = ByteBuffer.wrap(arr)
    encodeInto(bb, p)
    arr
  }
  
  def encodeInto(bb: ByteBuffer, p: KeyValueListPointer): Unit = {
    Varint.putUnsignedInt(bb, p.minimum.bytes.length)
    Varint.putUnsignedInt(bb, p.pointer.encodedSize)
    bb.put(p.minimum.bytes)
    ObjectPointer.encodeInto(bb, p.pointer)
  }
  
  def fromArray(arr: Array[Byte]): KeyValueListPointer = fromByteBuffer(ByteBuffer.wrap(arr))
  
  
  def fromByteBuffer(bb: ByteBuffer): KeyValueListPointer = {
    val minLen = Varint.getUnsignedInt(bb)
    val ptrLen = Varint.getUnsignedInt(bb)
    val minimum = new Array[Byte](minLen)
    bb.get(minimum)
    val endPosition = Some(bb.position + ptrLen)
    val pointer = ObjectPointer.fromByteBuffer(bb, endPosition)
    KeyValueListPointer(Key(minimum), pointer.asInstanceOf[KeyValueObjectPointer])
  }
  
}