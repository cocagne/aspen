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
  
  /** If size is None, the end of the array marks the end of the pointer */
  def fromArray(arr: Array[Byte], size: Option[Int]=None): DurableTaskPointer = size match {
    case None => new DurableTaskPointer(KeyValueObjectPointer(arr))
    case Some(sz) => new DurableTaskPointer(KeyValueObjectPointer(arr, sz))
  }
  
  /** If size is None, the limit of the byte buffer marks the end of the pointer */
  def fromByteBuffer(bb: ByteBuffer, size: Option[Int]=None): DurableTaskPointer = {
    new DurableTaskPointer(KeyValueObjectPointer(bb, size))
  }
}