package com.ibm.aspen.core.network

import com.ibm.aspen.core.read

trait ReadMessageReceiver {
  def send(message: read.Message): Unit
}