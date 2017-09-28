package com.ibm.aspen.core.network

import com.ibm.aspen.core.allocation.Allocate

trait StoreSideAllocationMessageReceiver {
  def receive(message: Allocate): Unit
}