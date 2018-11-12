package com.ibm.aspen.core.network

import com.ibm.aspen.core.read.{ReadResponse, TransactionCompletionResponse}

trait ClientSideReadMessageReceiver {
  def receive(message: ReadResponse): Unit
  def receive(message: TransactionCompletionResponse): Unit
}