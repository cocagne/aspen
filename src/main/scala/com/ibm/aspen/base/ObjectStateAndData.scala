package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp

case class ObjectStateAndData(pointer: ObjectPointer, revision:ObjectRevision, refcount:ObjectRefcount, timestamp: HLCTimestamp, data: DataBuffer)
