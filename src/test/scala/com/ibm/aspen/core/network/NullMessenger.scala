package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.Message

class NullMessenger extends Messenger {
  def send(toStore: DataStoreID, message: Message): Unit = ()
}