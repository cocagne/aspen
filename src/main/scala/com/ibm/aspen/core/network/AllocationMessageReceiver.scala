package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.allocation

trait AllocationMessageReceiver {
  def receive(message: allocation.Message): Unit
}