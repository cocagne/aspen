package com.ibm.aspen.core.network

trait ClientSideTransactionHandler extends ClientSideTransactionMessenger {
  def setReceiver(receiver: ClientSideTransactionMessageReceiver): Unit
}