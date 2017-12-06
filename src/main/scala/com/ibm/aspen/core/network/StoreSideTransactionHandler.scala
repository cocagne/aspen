package com.ibm.aspen.core.network

trait StoreSideTransactionHandler extends StoreSideTransactionMessenger {
  def setReceiver(receiver: StoreSideTransactionMessageReceiver): Unit 
}