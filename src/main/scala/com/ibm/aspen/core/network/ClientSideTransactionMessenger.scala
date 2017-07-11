package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.transaction.TxAcceptResponse

trait ClientSideTransactionMessenger {
  def receive(fromStore: DataStoreID, acceptResponse: TxAcceptResponse): Unit
  def receive(fromStore: DataStoreID, finalized: TxFinalized): Unit
}