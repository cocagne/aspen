package com.ibm.aspen.core.transaction

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.transaction.paxos.Learner
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.ClientSideTransactionMessageReceiver
import com.ibm.aspen.core.data_store.DataStoreID

class ClientTransactionManager(
    messenger: ClientSideTransactionMessenger,
    chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions
    val defaultDriverFactory: ClientTransactionDriver.Factory
    ) extends ClientSideTransactionMessageReceiver {
  import ClientTransactionManager._
  
  private[this] var transactions = Map[UUID, ClientTransactionDriver]()
  
  def runTransaction(
      transactionUUID: UUID, 
      startTimestamp: Long, 
      dataUpdates: List[DataUpdate],
      updateData: List[Map[Byte,ByteBuffer]],
      refcountUpdates: List[RefcountUpdate],
      finalizationActions: List[SerializedFinalizationAction],
      driverFactory: Option[(ClientSideTransactionMessenger, TransactionDescription, List[Map[Byte,ByteBuffer]]) => ClientTransactionDriver]) = {
    
    val objectsIterator = dataUpdates.iterator.map(_.objectPointer) ++ refcountUpdates.iterator.map(_.objectPointer)
    val primaryObject = objectsIterator.reduce( (op1, op2) => if (op1.ida.failureTolerance > op2.ida.failureTolerance) op1 else op2 )
   
    val designatedLeader = chooseDesignatedLeader(primaryObject)
    
    val txd = TransactionDescription(transactionUUID, startTimestamp, primaryObject, designatedLeader, 
                                     dataUpdates, refcountUpdates, finalizationActions, Some(messenger.client))
                                     
    val td = driverFactory.getOrElse(defaultDriverFactory)(messenger, txd, updateData)
    
    synchronized {
      transactions += (transactionUUID -> td)
    }
    
  }
  
  def receive(fromStore: DataStoreID, acceptResponse: TxAcceptResponse): Unit = {
    val otd = synchronized { transactions.get(acceptResponse.transactionUUID) }
    otd.foreach( td => td.receive(fromStore, acceptResponse) )
  }
  def receive(fromStore: DataStoreID, finalized: TxFinalized): Unit = {
    val otd = synchronized { transactions.get(finalized.transactionUUID) }
    otd.foreach( td => td.receive(fromStore, finalized) )
  }
}

object ClientTransactionManager {
  
}