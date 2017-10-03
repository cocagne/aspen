package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.allocation.AllocationErrors

sealed abstract class AllocationError extends Throwable

case class InsufficientOnlineNodes(required: Int, available: Int) extends AllocationError

case class StoreAllocationError(
    allocInto: ObjectPointer,
    allocIntoRevision: ObjectRevision,
    poolUUID: UUID, 
    minimumSize: Int,
    storeErrors: Map[Byte, AllocationErrors.Value]) extends AllocationError