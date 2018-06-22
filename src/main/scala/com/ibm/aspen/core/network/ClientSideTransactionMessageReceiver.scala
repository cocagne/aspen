package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxPrepareResponse

trait ClientSideTransactionMessageReceiver {
  def receive(prepareResponse: TxPrepareResponse): Unit
  def receive(acceptResponse: TxAcceptResponse): Unit
  def receive(resolved: TxResolved): Unit
  def receive(finalized: TxFinalized): Unit
}