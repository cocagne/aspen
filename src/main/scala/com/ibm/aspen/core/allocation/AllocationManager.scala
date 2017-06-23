package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.AllocationMessageReceiver

class AllocationManager(
    val clientManager: Option[ClientAllocationManager],
    val storeManager: Option[StoreAllocationManager]) extends AllocationMessageReceiver {
  
  def receive(message: Message): Unit = message match {
    case m: Allocate => storeManager.foreach( mgr => mgr.receiveAllocateMessage(m) )
    case m: AllocateResponse => clientManager.foreach( mgr => mgr.receiveAllocateResponseMessage(m) )
  }
}