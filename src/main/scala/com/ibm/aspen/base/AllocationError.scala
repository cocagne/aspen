package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.allocation.AllocationErrors

case class AllocationError(
    allocInto: ObjectPointer,
    allocIntoRevision: ObjectRevision,
    poolUUID: UUID, 
    minimumSize: Int,
    storeErrors: Map[Byte, AllocationErrors.Value])