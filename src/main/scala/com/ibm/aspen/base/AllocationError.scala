package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.allocation.AllocationErrors
import com.ibm.aspen.core.ida.IDA

sealed abstract class AllocationError extends Throwable

case class InsufficientOnlineNodes(required: Int, available: Int) extends AllocationError

case class UnsupportedIDA(poolUUID: UUID, ida:IDA) extends AllocationError

case class StoreAllocationError(
    allocatingObject: ObjectPointer,
    allocatingObjectRevision: ObjectRevision,
    poolUUID: UUID, 
    objectSize: Option[Int],
    objectIDA: IDA,
    storeErrors: Map[Byte, AllocationErrors.Value]) extends AllocationError