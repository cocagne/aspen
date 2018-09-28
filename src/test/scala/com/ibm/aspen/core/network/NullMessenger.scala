package com.ibm.aspen.core.network

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.transaction._

class NullMessenger extends StoreSideTransactionMessenger {
  def system: Option[AspenSystem] = None
  override def send(message: Message): Unit = ()
  def send(client: ClientID, prepareResponse: TxPrepareResponse): Unit = ()
  override def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit = ()
  override def send(client: ClientID, resolved: TxResolved): Unit = ()
  override def send(client: ClientID, finalized: TxFinalized): Unit = ()
  
  def sendPrepare(message: TxPrepare, updateContent: Option[List[LocalUpdate]] = None): Unit = ()
}