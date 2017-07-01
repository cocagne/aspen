package com.ibm.aspen.core.network

import com.ibm.aspen.core.read

trait StoreSideReadMessenger {
  def send(client: Client, message: read.Message): Unit
}