package com.ibm.aspen.core.network

import com.ibm.aspen.core.read
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer

trait StoreSideReadMessenger {
  def send(client: ClientID, message: read.ReadResponse): Unit
  def send(client: ClientID, message: read.TransactionCompletionResponse): Unit
}