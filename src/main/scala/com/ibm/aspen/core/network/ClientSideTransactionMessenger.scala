package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.transaction.LocalUpdateContent

trait ClientSideTransactionMessenger {
  def send(toStore: DataStoreID, message: TxPrepare, updateContent: Option[LocalUpdateContent] = None): Unit
  def receive(fromStore: DataStoreID, acceptResponse: TxAcceptResponse): Unit
  def receive(fromStore: DataStoreID, finalized: TxFinalized): Unit
  
  /** Identifies the local Client associated with this instance */
  def client: Client
}