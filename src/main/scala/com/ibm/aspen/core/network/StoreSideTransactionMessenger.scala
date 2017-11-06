package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.Message
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxFinalized
import java.nio.ByteBuffer

trait StoreSideTransactionMessenger {
  def send(message: Message, updateContent: Option[Array[ByteBuffer]] = None): Unit
  def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit
  def send(client: ClientID, finalized: TxFinalized): Unit
  
  def send(messages: List[(Message, Option[Array[ByteBuffer]])]): Unit = messages.foreach(t => send(t._1, t._2))
}