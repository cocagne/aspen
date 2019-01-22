package com.ibm.aspen.core.objects

import java.util.UUID
import java.nio.ByteBuffer

/** Object Revisions are set to the UUID of the transaction that last updated them
 * 
 */
final class ObjectRevision(val lastUpdateTxUUID: UUID) extends AnyVal {
  override def toString(): String = lastUpdateTxUUID.toString
  
  def toArray(): Array[Byte] = {
    val arr = new Array[Byte](16)
    encodeInto(ByteBuffer.wrap(arr))
    arr
  }
  
  def encodeInto(bb: ByteBuffer): ByteBuffer = {
    bb.putLong(lastUpdateTxUUID.getMostSignificantBits)
    bb.putLong(lastUpdateTxUUID.getLeastSignificantBits)
  }
}

object ObjectRevision {
  def apply(lastUpdateTxUUID: UUID): ObjectRevision = new ObjectRevision(lastUpdateTxUUID)
  
  def apply(arr: Array[Byte]): ObjectRevision = {
    val bb = ByteBuffer.wrap(arr)
    ObjectRevision(bb)
  }
  
  def apply(bb: ByteBuffer): ObjectRevision = {
    val msb = bb.getLong()
    val lsb = bb.getLong()
    new ObjectRevision(new UUID(msb, lsb))
  }
  
  val Null = ObjectRevision(new UUID(0,0))

  val EncodedSize: Int = 16
}