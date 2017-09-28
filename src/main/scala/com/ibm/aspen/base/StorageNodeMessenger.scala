package com.ibm.aspen.base

import com.ibm.aspen.core.network.StoreSideTransactionMessageReceiver
import com.ibm.aspen.core.network.ReadMessageReceiver

trait StorageNodeMessenger {
  def setTransactionMessageReceiver(receiver: StoreSideTransactionMessageReceiver): Unit
  def setReadMessageReceiver(receiver: ReadMessageReceiver): Unit
}