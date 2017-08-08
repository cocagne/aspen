package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectPointer

case class ObjectState(pointer: ObjectPointer, revision:ObjectRevision, refcount:ObjectRefcount)