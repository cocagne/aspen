package com.ibm.aspen.core.objects

import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.keyvalue.KVState

sealed abstract class ObjectState(
    val pointer: ObjectPointer, 
    val revision:ObjectRevision, 
    val refcount:ObjectRefcount, 
    val timestamp: HLCTimestamp)
    
class DataObjectState(
    pointer: ObjectPointer, 
    revision:ObjectRevision, 
    refcount:ObjectRefcount, 
    timestamp: HLCTimestamp, 
    val data: DataBuffer) extends ObjectState(pointer, revision, refcount, timestamp)

object DataObjectState {
  def apply(
      pointer: ObjectPointer, 
      revision:ObjectRevision, 
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp, 
      data: DataBuffer): DataObjectState = new DataObjectState(pointer, revision, refcount, timestamp, data)
}

class KeyValueObjectState(
    pointer: ObjectPointer, 
    revision:ObjectRevision, 
    refcount:ObjectRefcount, 
    timestamp: HLCTimestamp,
    val minimum: Option[Array[Byte]],
    val maximum: Option[Array[Byte]],
    val left: Option[Array[Byte]],
    val right: Option[Array[Byte]],
    val contents: Map[Array[Byte], KVState]
    ) extends ObjectState(pointer, revision, refcount, timestamp)

object KeyValueObjectState {
  def apply(
      pointer: ObjectPointer, 
      revision:ObjectRevision, 
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp, 
      minimum: Option[Array[Byte]],
      maximum: Option[Array[Byte]],
      left: Option[Array[Byte]],
      right: Option[Array[Byte]],
      contents: Map[Array[Byte], KVState]): KeyValueObjectState = {
    new KeyValueObjectState(pointer, revision, refcount, timestamp, minimum, maximum, left, right, contents)
  }
}
