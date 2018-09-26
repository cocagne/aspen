package com.ibm.aspen.core.network

import com.ibm.aspen.core.transaction._

trait StoreSideTransactionMessenger {
  
  def send(message: Message): Unit
  def send(client: ClientID, prepareResponse: TxPrepareResponse): Unit
  def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit
  def send(client: ClientID, resolved: TxResolved): Unit
  def send(client: ClientID, finalized: TxFinalized): Unit
  
  def sendPrepare(message: TxPrepare, updateContent: Option[List[LocalUpdate]] = None): Unit
  
  def send(messages: List[Message]): Unit = messages.foreach(m => send(m))
  def sendPrepares(messages: List[(TxPrepare, Option[List[LocalUpdate]])]): Unit = messages.foreach(t => sendPrepare(t._1, t._2))
}