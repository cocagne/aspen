package com.ibm.aspen.core.network

import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.allocation.AllocationStatusRequest
import com.ibm.aspen.core.allocation.AllocationStatusReply

trait StoreSideAllocationMessageReceiver {
  def receive(message: Allocate): Unit
  def receive(message: AllocationStatusRequest): Unit
  def receive(message: AllocationStatusReply): Unit
}