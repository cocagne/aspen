package com.ibm.aspen.core.network

import com.ibm.aspen.core.read
import com.ibm.aspen.core.data_store.DataStoreID

trait ClientSideReadMessenger {
  def send(toStore: DataStoreID, message: read.Read): Unit
  
  /** Identifies the local Client associated with this instance */
  val clientId: ClientID
}