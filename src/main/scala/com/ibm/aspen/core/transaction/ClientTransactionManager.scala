package com.ibm.aspen.core.transaction

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.transaction.paxos.Learner
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.ClientSideTransactionMessageReceiver
import com.ibm.aspen.core.data_store.DataStoreID
import scala.concurrent.Future

class ClientTransactionManager(
    messenger: ClientSideTransactionMessenger,
    val defaultDriverFactory: ClientTransactionDriver.Factory
    ) extends ClientSideTransactionMessageReceiver {
  import ClientTransactionManager._
  
  val clientId = messenger.clientId
  
  private[this] var transactions = Map[UUID, ClientTransactionDriver]()
  
  def runTransaction(
      txd: TransactionDescription,
      updateData: List[Map[Byte,ByteBuffer]],
      driverFactory: Option[ClientTransactionDriver.Factory]): Future[Boolean] = {
                                 
    val td = driverFactory.getOrElse(defaultDriverFactory)(messenger, txd, updateData)
    
    synchronized {
      transactions += (txd.transactionUUID -> td)
    }
    
    td.result
  }
  
  def receive(acceptResponse: TxAcceptResponse): Unit = {
    val otd = synchronized { transactions.get(acceptResponse.transactionUUID) }
    otd.foreach( td => td.receive(acceptResponse) )
  }
  def receive(finalized: TxFinalized): Unit = {
    val otd = synchronized { transactions.get(finalized.transactionUUID) }
    otd.foreach( td => td.receive(finalized) )
  }
}

object ClientTransactionManager {
  
}