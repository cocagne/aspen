package com.ibm.aspen.core.network

import com.ibm.aspen.core.read

trait ReadMessageReceiver {
  def receive(message: read.Message): Unit
}