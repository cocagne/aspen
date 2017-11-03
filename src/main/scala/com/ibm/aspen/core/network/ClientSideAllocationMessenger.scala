package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.allocation

trait ClientSideAllocationMessenger {
  def send(toStore: DataStoreID, message: allocation.Allocate): Unit
  
  /** Identifies the local Client associated with this instance */
  val clientId: ClientID
}