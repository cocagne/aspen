package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer

case class ObjectStateAndData(pointer: ObjectPointer, revision:ObjectRevision, refcount:ObjectRefcount, data: ByteBuffer)
