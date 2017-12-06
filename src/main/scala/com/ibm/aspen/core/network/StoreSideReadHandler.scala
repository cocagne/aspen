package com.ibm.aspen.core.network

trait StoreSideReadHandler extends StoreSideReadMessenger {
  def setReceiver(receiver: StoreSideReadMessageReceiver): Unit
}