package com.ibm.aspen.core.network

import com.ibm.aspen.core.transaction.Message
import com.ibm.aspen.core.transaction.LocalUpdateContent
import com.ibm.aspen.core.data_store.DataStoreID

trait MessageReceiver {
  def receive(toStore: DataStoreID, message: Message, updateContent: Option[LocalUpdateContent]): Unit  
}