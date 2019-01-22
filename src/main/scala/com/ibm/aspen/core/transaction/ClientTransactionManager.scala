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
  
  val clientId = messenger.clientId
  
  private[this] var transactions = Map[UUID, ClientTransactionDriver]()
  
  /** Immediately cancels all activity scheduled for future execution */
  def shutdown(): Unit = transactions.foreach( t => t._2.shutdown() )
  
  def runTransaction(
      txd: TransactionDescription,
      updateData: Map[DataStoreID, (List[LocalUpdate], List[PreTransactionOpportunisticRebuild])],
      driverFactory: Option[ClientTransactionDriver.Factory]): Future[Boolean] = {
                                 
    val td = driverFactory.getOrElse(defaultDriverFactory)(messenger, txd, updateData)
    
    synchronized {
      transactions += (txd.transactionUUID -> td)
    }
    
    td.begin()
    
    td.result
  }
  
  def receive(prepareResponse: TxPrepareResponse): Unit= {
    val otd = synchronized { transactions.get(prepareResponse.transactionUUID) }
    otd.foreach( td => td.receive(prepareResponse) )
  } 
  def receive(acceptResponse: TxAcceptResponse): Unit = {
    val otd = synchronized { transactions.get(acceptResponse.transactionUUID) }
    otd.foreach( td => td.receive(acceptResponse) )
  }
  def receive(resolved: TxResolved): Unit = {
    val otd = synchronized { transactions.get(resolved.transactionUUID) }
    otd.foreach( td => td.receive(resolved) )
  }
  def receive(finalized: TxFinalized): Unit = {
    val otd = synchronized { transactions.get(finalized.transactionUUID) }
    otd.foreach( td => td.receive(finalized) )
  }
}

