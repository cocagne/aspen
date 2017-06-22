package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.allocation

trait AllocationMessenger {
  def send(toStore: DataStoreID, message: allocation.Message): Unit
  
  /** Identifies the local Client associated with this instance */
  def client: Client
}