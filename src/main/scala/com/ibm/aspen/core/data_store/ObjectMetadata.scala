package com.ibm.aspen.core.data_store

import java.nio.ByteBuffer

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision

case class ObjectMetadata(
    revision: ObjectRevision,
    refcount: ObjectRefcount,
    timestamp: HLCTimestamp) {

  def toArray: Array[Byte] = {
    val arr = new Array[Byte](ObjectMetadata.EncodedSize)
    encodeInto(ByteBuffer.wrap(arr))
    arr
  }

  def encodeInto(bb: ByteBuffer): ByteBuffer = {
    revision.encodeInto(bb)
    refcount.encodeInto(bb)
    bb.putLong(timestamp.asLong)
  }
}

object ObjectMetadata {
  val EncodedSize: Int = ObjectRevision.EncodedSize + ObjectRefcount.EncodedSize + HLCTimestamp.EncodedSize

  def apply(arr: Array[Byte]): ObjectMetadata = ObjectMetadata(ByteBuffer.wrap(arr))

  def apply(bb: ByteBuffer): ObjectMetadata = {
    val revision = ObjectRevision(bb)
    val refcount = ObjectRefcount(bb)
    val timestamp = HLCTimestamp(bb.getLong())
    new ObjectMetadata(revision, refcount, timestamp)
  }
}