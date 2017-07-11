package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.Message
import com.ibm.aspen.core.transaction.LocalUpdateContent
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxFinalized

class NullMessenger extends TransactionMessenger {
  def send(toStore: DataStoreID, message: Message, updateContent: Option[LocalUpdateContent]): Unit = ()
  def send(client: Client, acceptResponse: TxAcceptResponse): Unit = ()
  def send(client: Client, finalized: TxFinalized): Unit = ()
}