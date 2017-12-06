package com.ibm.aspen.core.network

trait StoreSideAllocationHandler extends StoreSideAllocationMessenger {
  def setReceiver(receiver: StoreSideAllocationMessageReceiver): Unit
}