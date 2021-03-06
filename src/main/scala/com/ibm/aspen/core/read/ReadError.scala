package com.ibm.aspen.core.read

import com.ibm.aspen.base.AspenError
import com.ibm.aspen.core.objects.ObjectPointer

sealed abstract class ReadError(msg: String) extends AspenError(msg) {
  val pointer: ObjectPointer
  
  override def toString(): String = s"${this.getClass.getSimpleName}(${pointer.getClass.getSimpleName}:${pointer.uuid})"
}


sealed abstract class FatalReadError(msg: String) extends ReadError(msg)

/** Thrown when the requested object is not found. */
class InvalidObject(val pointer: ObjectPointer) extends FatalReadError("InvalidObject")

/** Thrown when the object content becomes corrupted. */
class CorruptedObject(val pointer: ObjectPointer) extends FatalReadError("CorruptedObject")

/** Thrown when IDA decoding fails */
class CorruptedIDA(pointer: ObjectPointer) extends CorruptedObject(pointer)

/** Thrown when the content of a complex object such as a KeyValue object becomes corrupted
 *  and cannot be decoded.
 */
class CorruptedContent(pointer: ObjectPointer) extends CorruptedObject(pointer)



sealed abstract class TransientReadError(msg: String) extends ReadError(msg)

class ReadTimeout(val pointer: ObjectPointer) extends TransientReadError("ReadTimeout")

