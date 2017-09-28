package com.ibm.aspen.core.network

import com.ibm.aspen.core.allocation.AllocateResponse

trait ClientSideAllocationMessageReceiver {
  def receive(message: AllocateResponse): Unit
}