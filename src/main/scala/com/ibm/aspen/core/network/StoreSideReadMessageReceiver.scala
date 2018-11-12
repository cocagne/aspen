package com.ibm.aspen.core.network

import com.ibm.aspen.core.read.{OpportunisticRebuild, Read, TransactionCompletionQuery}

trait StoreSideReadMessageReceiver {
  def receive(message: Read): Unit
  def receive(message: OpportunisticRebuild): Unit
  def receive(message: TransactionCompletionQuery): Unit
}