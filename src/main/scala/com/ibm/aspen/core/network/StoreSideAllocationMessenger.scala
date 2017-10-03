package com.ibm.aspen.core.network

import com.ibm.aspen.core.allocation

trait StoreSideAllocationMessenger {
  def send(client: ClientID, message: allocation.Message): Unit
}