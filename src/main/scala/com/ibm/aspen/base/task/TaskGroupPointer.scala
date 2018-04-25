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
  
  /** If size is None, the end of the array marks the end of the pointer */
  def fromArray(arr: Array[Byte]): TaskGroupPointer = new TaskGroupPointer(KeyValueObjectPointer(arr))
    
  def fromByteBuffer(bb: ByteBuffer): TaskGroupPointer = new TaskGroupPointer(KeyValueObjectPointer(bb))
  
}