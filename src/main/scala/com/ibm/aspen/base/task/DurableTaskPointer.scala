package com.ibm.aspen.base.task

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer

final class DurableTaskPointer(val kvPointer: KeyValueObjectPointer) extends AnyVal {
  def toArray: Array[Byte] = kvPointer.toArray
  
  def encodedSize: Int = kvPointer.encodedSize
  
  def encodeInto(bb: ByteBuffer): Unit = kvPointer.encodeInto(bb)
}

object DurableTaskPointer {
  def apply(kvPointer: KeyValueObjectPointer): DurableTaskPointer = new DurableTaskPointer(kvPointer)
  
  def fromArray(arr: Array[Byte], endPosition: Option[Int]=None): DurableTaskPointer = {
    new DurableTaskPointer(ObjectPointer.fromArray(arr, endPosition).asInstanceOf[KeyValueObjectPointer])
  }
  
  def fromByteBuffer(bb: ByteBuffer, endPosition: Option[Int]=None): DurableTaskPointer = {
    new DurableTaskPointer(ObjectPointer.fromByteBuffer(bb, endPosition).asInstanceOf[KeyValueObjectPointer])
  }
}