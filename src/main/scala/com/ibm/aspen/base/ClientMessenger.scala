package com.ibm.aspen.base

import com.ibm.aspen.core.network.ClientSideAllocationMessenger
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.network.ClientSideTransactionMessageReceiver

trait ClientMessenger extends ClientSideAllocationMessenger with ClientSideReadMessenger with ClientSideTransactionMessenger {
  
  def setTransactionMessageReceiver(receiver: ClientSideTransactionMessageReceiver): Unit
}