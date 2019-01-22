package com.ibm.aspen.core.network

import com.ibm.aspen.core.transaction._

trait ClientSideTransactionMessenger {
  def send(message: TxPrepare, transactionData: TransactionData): Unit
  
  /** Identifies the local Client associated with this instance */
  val clientId: ClientID
}