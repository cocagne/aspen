package com.ibm.aspen.core.network

import com.ibm.aspen.core.read.Read

trait StoreSideReadMessageReceiver {
  def receive(message: Read): Unit
}