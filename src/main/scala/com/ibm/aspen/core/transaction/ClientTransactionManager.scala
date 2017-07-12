package com.ibm.aspen.core.transaction

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.ClientSideTransactionMessenger
import com.ibm.aspen.core.transaction.paxos.Learner

class ClientTransactionManager(
    messenger: ClientSideTransactionMessenger,
    chooseDesignatedLeader: (ObjectPointer) => Byte // Uses peer online/offline knowledge to select designated leaders for transactions
    ) {
  import ClientTransactionManager._
  
  private[this] var transactions = Map[UUID, TransactionState]()
  
  def runTransaction(
      transactionUUID: UUID, 
      startTimestamp: Long, 
      dataUpdates: List[DataUpdate],
      refcountUpdates: List[RefcountUpdate],
      finalizationActions: List[SerializedFinalizationAction]) = {
    
    val objectsIterator = dataUpdates.iterator.map(_.objectPointer) ++ refcountUpdates.iterator.map(_.objectPointer)
    val primaryObject = objectsIterator.reduce( (op1, op2) => if (op1.ida.failureTolerance > op2.ida.failureTolerance) op1 else op2 )
   
    val designatedLeader = chooseDesignatedLeader(primaryObject)
    
    val txd = TransactionDescription(transactionUUID, startTimestamp, primaryObject, designatedLeader, 
                                     dataUpdates, refcountUpdates, finalizationActions, Some(messenger.client))
                                     
    val ts = new TransactionState(txd)
    
    synchronized {
      transactions += (transactionUUID -> ts)
    }
    
  }
}

object ClientTransactionManager {
  private class TransactionState(val txd: TransactionDescription) {
    val learner = new Learner(txd.primaryObject.ida.width, txd.primaryObject.ida.writeThreshold) 
  }
}