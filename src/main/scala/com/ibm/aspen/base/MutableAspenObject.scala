package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.transaction.DataUpdate
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.RefcountUpdate

trait MutableAspenObject extends AspenObject {
  
  def refcount_=(newRefcount: ObjectRefcount)
  
  /** Writes min(this.maxSize, buf.length, length) bytes of buf to the object content beginning at the specified offset
   *  Puts that exceed the current size of the object will extend the object size up to maxSize. An IndexOutOfBounds
   *  exception will be thrown if an attempt is made to exceed maxSize
   */
  def put(byteIndex: Int, buf: Array[Byte], length: Option[Int]=None): Unit
  
  /** The following methods return a numeric of the specified type or throw an IndexOutOfBounds exception if
   *  a write at the requested index location exceeds the size of the object or occurs beyond the current size
   *  of the object. Put operations performed at the this.size index will extend the current size of the object
   *  if this.size is less than this.maxSize
   */
  def putByte(byteIndex: Int, data: Byte): Unit
  def putShort(byteIndex: Int, data: Short): Unit
  def putInt(byteIndex: Int, data: Int): Unit
  def putLong(byteIndex: Int, data: Long): Unit
  def putFloat(byteIndex: Int, data: Float): Unit
  def putDouble(byteIndex: Int, data: Double): Unit
  
  private [base] def dataUpdate(): Option[(DataUpdate, ByteBuffer)]
  private [base] def refcountUpdate(): Option[RefcountUpdate]
}