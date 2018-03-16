package com.ibm.aspen.base.task

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer

final class TaskGroupPointer(val kvPointer: KeyValueObjectPointer) extends AnyVal {
  def toArray: Array[Byte] = kvPointer.toArray
  
  def encodedSize: Int = kvPointer.encodedSize
  
  def encodeInto(bb: ByteBuffer): Unit = kvPointer.encodeInto(bb)
}

object TaskGroupPointer {
  def apply(kvPointer: KeyValueObjectPointer): TaskGroupPointer = new TaskGroupPointer(kvPointer)
  
  def fromArray(arr: Array[Byte], endPosition: Option[Int]=None): TaskGroupPointer = {
    new TaskGroupPointer(ObjectPointer.fromArray(arr, endPosition).asInstanceOf[KeyValueObjectPointer])
  }
  
  def fromByteBuffer(bb: ByteBuffer, endPosition: Option[Int]=None): TaskGroupPointer = {
    new TaskGroupPointer(ObjectPointer.fromByteBuffer(bb, endPosition).asInstanceOf[KeyValueObjectPointer])
  }
}