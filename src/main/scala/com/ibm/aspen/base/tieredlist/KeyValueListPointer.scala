package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.util.Varint
import com.ibm.aspen.core.objects.KeyValueObjectState

final case class KeyValueListPointer(minimum:Key, pointer:KeyValueObjectPointer) {
  
  import KeyValueListPointer._
  
  def toArray: Array[Byte] = encodeToByteArray(this)
  
  def encodedSize: Int = {
    val ptrSize = pointer.encodedSize
    
    Varint.getUnsignedIntEncodingLength(minimum.bytes.length) + 
    minimum.bytes.length +
    ptrSize
  }
}

object KeyValueListPointer {

  def apply(objectState: KeyValueObjectState): KeyValueListPointer = {
    new KeyValueListPointer(objectState.minimum.getOrElse(Key.AbsoluteMinimum), objectState.pointer)
  }
  
  def encodeToByteArray(p: KeyValueListPointer): Array[Byte] = {
    val arr = new Array[Byte](p.encodedSize)
    val bb = ByteBuffer.wrap(arr)
    encodeInto(bb, p)
    arr
  }
  
  def encodeInto(bb: ByteBuffer, p: KeyValueListPointer): Unit = {
    Varint.putUnsignedInt(bb, p.minimum.bytes.length)
    bb.put(p.minimum.bytes)
    ObjectPointer.encodeInto(bb, p.pointer)
  }
  
  def fromArray(arr: Array[Byte]): KeyValueListPointer = fromByteBuffer(ByteBuffer.wrap(arr))
  
  
  def fromByteBuffer(bb: ByteBuffer): KeyValueListPointer = {
    val minLen = Varint.getUnsignedInt(bb)
    val minimum = new Array[Byte](minLen)
    bb.get(minimum)
    KeyValueListPointer(Key(minimum), KeyValueObjectPointer(bb))
  }
  
}