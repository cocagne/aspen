package com.ibm.aspen.base

import com.ibm.aspen.core.network.StoreSideTransactionMessageReceiver
import com.ibm.aspen.core.network.StoreSideReadMessageReceiver
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.network.StoreSideAllocationMessenger
import com.ibm.aspen.core.network.StoreSideReadMessenger

trait StorageNodeMessenger extends StoreSideTransactionMessenger with StoreSideAllocationMessenger with StoreSideReadMessenger {
  def setMessageReceivers(
      transactionMessageReceiver: StoreSideTransactionMessageReceiver,
      readMessageReceiver: StoreSideReadMessageReceiver): Unit
}