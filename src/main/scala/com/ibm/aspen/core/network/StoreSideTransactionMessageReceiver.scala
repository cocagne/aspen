package com.ibm.aspen.core.network

import com.ibm.aspen.core.transaction.{Message, TransactionData}

trait StoreSideTransactionMessageReceiver {
  def receive(message: Message, updateContent: Option[TransactionData]): Unit
}