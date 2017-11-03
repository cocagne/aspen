package com.ibm.aspen.base.impl

import com.ibm.aspen.core.network.ClientSideAllocationMessenger
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.network.ClientSideTransactionMessageReceiver
import com.ibm.aspen.core.network.ClientSideReadMessageReceiver
import com.ibm.aspen.core.network.ClientSideAllocationMessageReceiver
import com.ibm.aspen.core.allocation.AllocateResponse
import com.ibm.aspen.core.read.ReadResponse
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxFinalized

trait ClientMessenger extends ClientSideAllocationMessenger with ClientSideReadMessenger with ClientSideTransactionMessenger with
                              ClientSideAllocationMessageReceiver with ClientSideReadMessageReceiver with ClientSideTransactionMessageReceiver {
  
  private [this] var txReceiver: Option[ClientSideTransactionMessageReceiver] = None
  private [this] var readReceiver: Option[ClientSideReadMessageReceiver] = None
  private [this] var allocReceiver: Option[ClientSideAllocationMessageReceiver] = None
  
  def setMessageReceivers(
      transactionMessageReceiver: ClientSideTransactionMessageReceiver,
      readMessageReceiver: ClientSideReadMessageReceiver,
      allocationMessageReceiver: ClientSideAllocationMessageReceiver): Unit = synchronized {
    txReceiver = Some(transactionMessageReceiver)
    readReceiver = Some(readMessageReceiver)
    allocReceiver = Some(allocationMessageReceiver)
  }
      
  def receive(message: AllocateResponse): Unit = synchronized { allocReceiver } foreach { r => r.receive(message) }
  def receive(message: ReadResponse): Unit = synchronized { readReceiver } foreach { r => r.receive(message) }
  def receive(acceptResponse: TxAcceptResponse): Unit = synchronized { txReceiver } foreach { r => r.receive(acceptResponse) }
  def receive(finalized: TxFinalized): Unit = synchronized { txReceiver } foreach { r => r.receive(finalized) }
  
}