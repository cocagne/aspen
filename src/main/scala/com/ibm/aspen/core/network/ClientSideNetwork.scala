package com.ibm.aspen.core.network

trait ClientSideNetwork {
  val readHandler: ClientSideReadHandler
  val allocationHandler: ClientSideAllocationHandler
  val transactionHandler: ClientSideTransactionHandler
  
  def clientId = readHandler.clientId
}