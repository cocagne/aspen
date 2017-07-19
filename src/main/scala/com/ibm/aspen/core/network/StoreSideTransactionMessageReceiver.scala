package com.ibm.aspen.core.network

import com.ibm.aspen.core.transaction.Message
import com.ibm.aspen.core.data_store.DataStoreID
import java.nio.ByteBuffer

trait StoreSideTransactionMessageReceiver {
  def receive(fromStore: DataStoreID, message: Message, updateContent: Option[Array[ByteBuffer]]): Unit  
}