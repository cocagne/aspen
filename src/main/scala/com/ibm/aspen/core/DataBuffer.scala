package com.ibm.aspen.core

import java.nio.ByteBuffer
import scala.language.implicitConversions

/** This class serves as a Read-Only, compile-time wrapper around ByteBuffer. 
 *  
 *  ByteBuffer is mutable and it's very easy to forget to save and restore the position variable when reading data buffer.
 *  This class serves primarily to guard against accidental mutation of the ByteBuffer attributes and content.
 *  
 *  Implemented as an AnyVal so no run-time allocation overhead is required by this wrapper.  
 *  
 */
final class DataBuffer private (private val buf: ByteBuffer) extends AnyVal {
  
  /** Creates a new read-only copy of the wrapped byte buffer */
  def asReadOnlyBuffer(): ByteBuffer = buf.asReadOnlyBuffer()
  
  def size: Int = buf.limit() - buf.position()
  
  def get(byteOffset: Int): Byte = buf.get(byteOffset)
  def getShort(byteOffset: Int): Short = buf.getShort(byteOffset)
  def getInt(byteOffset: Int): Int = buf.getInt(byteOffset)
  def getLong(byteOffset: Int): Long = buf.getLong(byteOffset)
  
  def getDouble(byteOffset: Int): Double = buf.getDouble(byteOffset)
  def getFloat(byteOffset: Int): Float = buf.getFloat(byteOffset)
  
  /** Creates a copy of the wrapped byte buffer content */
  def getByteArray(): Array[Byte] = {
    val arr = new Array[Byte](size)
    buf.asReadOnlyBuffer().get(arr)
    arr
  }
  
  def compareTo(that: DataBuffer): Int = buf.compareTo(that.buf)
  
}

object DataBuffer {
  implicit def apply(buf: ByteBuffer): DataBuffer = new DataBuffer(buf.asReadOnlyBuffer())
  implicit def apply(arr: Array[Byte]): DataBuffer = new DataBuffer(ByteBuffer.wrap(arr))
  implicit def db2bb(db: DataBuffer): ByteBuffer = db.asReadOnlyBuffer()
}