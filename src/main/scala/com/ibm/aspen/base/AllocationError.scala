package com.ibm.aspen.base

import java.util.UUID

import com.ibm.aspen.core.allocation.{AllocationErrors, AllocationRevisionGuard}
import com.ibm.aspen.core.ida.IDA

sealed abstract class AllocationError extends Throwable

case class InsufficientOnlineNodes(required: Int, available: Int) extends AllocationError

case class UnsupportedIDA(poolUUID: UUID, ida:IDA) extends AllocationError

case class StoreAllocationError(
                                 revisionGuard: AllocationRevisionGuard,
                                 poolUUID: UUID,
                                 objectSize: Option[Int],
                                 objectIDA: IDA,
                                 storeErrors: Map[Byte, AllocationErrors.Value]) extends AllocationError
    
case class ObjectSizeExceeded(maximumSize: Int, requestedSize: Int) extends AllocationError