package com.ibm.aspen.base.impl

import com.ibm.aspen.core.network.ClientSideAllocationMessenger
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.network.ClientSideTransactionMessageReceiver
import com.ibm.aspen.core.network.ClientSideReadMessageReceiver
import com.ibm.aspen.core.network.ClientSideAllocationMessageReceiver

trait ClientMessenger extends ClientSideAllocationMessenger with ClientSideReadMessenger with ClientSideTransactionMessenger {
  
  def setMessageReceivers(
      transactionMessageReceiver: ClientSideTransactionMessageReceiver,
      readMessageReceiver: ClientSideReadMessageReceiver,
      allocationMessageReceiver: ClientSideAllocationMessageReceiver): Unit
}