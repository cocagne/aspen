package com.ibm.aspen.core.network

trait ClientSideAllocationHandler extends ClientSideAllocationMessenger {
  def setReceiver(receiver: ClientSideAllocationMessageReceiver): Unit
}