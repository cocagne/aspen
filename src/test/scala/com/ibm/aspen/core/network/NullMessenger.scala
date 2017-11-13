package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.Message
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxFinalized
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.LocalUpdate

class NullMessenger extends StoreSideTransactionMessenger {
  override def send(message: Message, updateContent: Option[List[LocalUpdate]]): Unit = ()
  override def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit = ()
  override def send(client: ClientID, finalized: TxFinalized): Unit = ()
}