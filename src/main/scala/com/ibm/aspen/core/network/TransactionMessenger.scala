package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.Message
import com.ibm.aspen.core.transaction.LocalUpdateContent

trait TransactionMessenger {
  def send(toStore: DataStoreID, message: Message, updateContent: Option[LocalUpdateContent] = None): Unit
}