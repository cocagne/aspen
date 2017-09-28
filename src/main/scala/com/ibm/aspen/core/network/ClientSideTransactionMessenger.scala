package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxPrepare
import java.nio.ByteBuffer

trait ClientSideTransactionMessenger {
  def send(message: TxPrepare, updateContent: List[ByteBuffer]): Unit
  
  /** Identifies the local Client associated with this instance */
  val client: Client
}