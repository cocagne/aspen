package com.ibm.aspen.base

import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount

trait AspenObject {
  def objectPointer: ObjectPointer
  def uuid = objectPointer.uuid
  def poolUUID: UUID = objectPointer.poolUUID
  def maxSize: Option[Int] = objectPointer.size
  def ida: IDA = objectPointer.ida
  
  def revision: ObjectRevision
  def refcount: ObjectRefcount
  
  /** Size in bytes of the object */
  def size: Int
  
  /** Writes min(this.size, buf.length, length) bytes to buf beginning with object content at the specified offset */
  def get(byteIndex: Int, buf: Array[Byte], length: Option[Int]=None): Unit
  
  /** The following methods return a numeric of the specified type or throw an IndexOutOfBounds exception if
   *  a read at the requested index location exceeds the size of the object
   */
  def getByte(byteIndex: Int): Byte
  def getShort(byteIndex: Int): Short
  def getInt(byteIndex: Int): Int
  def getLong(byteIndex: Int): Long
  def getFloat(byteIndex: Int): Float
  def getDouble(byteIndex: Int): Double
  
}