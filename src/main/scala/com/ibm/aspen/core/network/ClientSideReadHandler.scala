package com.ibm.aspen.core.network

trait ClientSideReadHandler extends ClientSideReadMessenger {
  def setReceiver(receiver: ClientSideReadMessageReceiver): Unit
}