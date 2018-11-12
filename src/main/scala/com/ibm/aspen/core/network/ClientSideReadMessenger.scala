package com.ibm.aspen.core.network

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.read

trait ClientSideReadMessenger {
  def send(message: read.Read): Unit
  
  def send(msg: read.OpportunisticRebuild): Unit

  def send(msg: read.TransactionCompletionQuery): Unit

  def system: Option[AspenSystem]
  
  /** Identifies the local Client associated with this instance */
  val clientId: ClientID
}