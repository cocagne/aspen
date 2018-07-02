package com.ibm.aspen.core.network

import com.ibm.aspen.core.read.Read
import com.ibm.aspen.core.read.OpportunisticRebuild

trait StoreSideReadMessageReceiver {
  def receive(message: Read): Unit
  def receive(message: OpportunisticRebuild): Unit
}