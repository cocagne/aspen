package com.ibm.aspen.core.network

import com.ibm.aspen.core.transaction.Message
import com.ibm.aspen.core.data_store.DataStoreID
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.LocalUpdate

trait StoreSideTransactionMessageReceiver {
  def receive(message: Message, updateContent: Option[List[LocalUpdate]]): Unit  
}