package com.ibm.aspen.cumulofs

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer

final class InodePointer(val kvPointer: KeyValueObjectPointer) extends AnyVal {
  def toArray: Array[Byte] = kvPointer.toArray
  
  def encodedSize: Int = kvPointer.encodedSize
  
  def encodeInto(bb: ByteBuffer): Unit = kvPointer.encodeInto(bb)
}

object InodePointer {
  def apply(kvPointer: KeyValueObjectPointer): InodePointer = new InodePointer(kvPointer)
  
  def fromArray(arr: Array[Byte], endPosition: Option[Int]=None): InodePointer = {
    new InodePointer(ObjectPointer.fromArray(arr, endPosition).asInstanceOf[KeyValueObjectPointer])
  }
  
  def fromByteBuffer(bb: ByteBuffer, endPosition: Option[Int]=None): InodePointer = {
    new InodePointer(ObjectPointer.fromByteBuffer(bb, endPosition).asInstanceOf[KeyValueObjectPointer])
  }
}