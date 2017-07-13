package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxFinalized

trait ClientSideTransactionMessageReceiver {
  def receive(fromStore: DataStoreID, acceptResponse: TxAcceptResponse): Unit
  def receive(fromStore: DataStoreID, finalized: TxFinalized): Unit
}