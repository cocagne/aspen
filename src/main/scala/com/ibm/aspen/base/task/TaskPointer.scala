package com.ibm.aspen.base.task

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer

final class TaskPointer(val kvPointer: KeyValueObjectPointer) extends AnyVal {
  def toArray: Array[Byte] = kvPointer.toArray
  
  def encodedSize: Int = kvPointer.encodedSize
  
  def encodeInto(bb: ByteBuffer): Unit = kvPointer.encodeInto(bb)
}

object TaskPointer {
  def apply(kvPointer: KeyValueObjectPointer): TaskPointer = new TaskPointer(kvPointer)
  
  def fromArray(arr: Array[Byte], endPosition: Option[Int]=None): TaskPointer = {
    new TaskPointer(ObjectPointer.fromArray(arr, endPosition).asInstanceOf[KeyValueObjectPointer])
  }
  
  def fromByteBuffer(bb: ByteBuffer, endPosition: Option[Int]=None): TaskPointer = {
    new TaskPointer(ObjectPointer.fromByteBuffer(bb, endPosition).asInstanceOf[KeyValueObjectPointer])
  }
}