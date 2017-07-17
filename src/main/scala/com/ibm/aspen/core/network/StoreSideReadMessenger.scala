package com.ibm.aspen.core.network

import com.ibm.aspen.core.read
import java.nio.ByteBuffer

trait StoreSideReadMessenger {
  def send(client: Client, message: read.ReadResponse, data:Option[ByteBuffer]): Unit
}