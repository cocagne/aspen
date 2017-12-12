package com.ibm.aspen.core.network

import com.ibm.aspen.core.allocation
import com.ibm.aspen.core.allocation.AllocationStatusRequest
import com.ibm.aspen.core.allocation.AllocationStatusReply

trait StoreSideAllocationMessenger {
  def send(client: ClientID, message: allocation.ClientMessage): Unit
  def send(message: AllocationStatusRequest): Unit
  def send(message: AllocationStatusReply): Unit
}