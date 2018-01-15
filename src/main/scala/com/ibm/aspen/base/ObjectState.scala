package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp

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
