package com.ibm.aspen.base.keyvalue

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Key
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectPointer

final case class KeyValueListPointer(pointer:KeyValueObjectPointer, minimum:Key) {
  
  import KeyValueListPointer._
  
  def toArray: Array[Byte] = encodeToByteArray(this)
}

object KeyValueListPointer {
  
  def encodeToByteArray(p: KeyValueListPointer): Array[Byte] = {
    val arr = ObjectPointer.encodeToByteArray(p.pointer, Some(p.minimum.bytes.length))
    val bb = ByteBuffer.wrap(arr)
    bb.position(arr.length - p.minimum.bytes.length)
    bb.put(p.minimum.bytes)
    arr
  }
  
  def fromArray(arr: Array[Byte]): KeyValueListPointer = {
    val bb = ByteBuffer.wrap(arr)
    val pointer = ObjectPointer.fromByteBuffer(bb)
    val minimum = new Array[Byte](bb.remaining())
    bb.get(minimum)
    KeyValueListPointer(pointer.asInstanceOf[KeyValueObjectPointer], Key(minimum))
  }
  
}