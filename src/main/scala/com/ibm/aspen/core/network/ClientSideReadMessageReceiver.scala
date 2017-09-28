package com.ibm.aspen.core.network

import com.ibm.aspen.core.read.ReadResponse

trait ClientSideReadMessageReceiver {
  def receive(message: ReadResponse): Unit
}