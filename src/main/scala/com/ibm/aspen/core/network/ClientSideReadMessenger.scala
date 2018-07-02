package com.ibm.aspen.core.network

import com.ibm.aspen.core.read
import com.ibm.aspen.core.data_store.DataStoreID

trait ClientSideReadMessenger {
  def send(message: read.Read): Unit
  
  def send(msg: read.OpportunisticRebuild): Unit
  
  /** Identifies the local Client associated with this instance */
  val clientId: ClientID
}