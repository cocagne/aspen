package com.ibm.aspen.core.transaction


import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.data_store.DataStoreID
import scala.concurrent.Future

trait TransactionFinalizer {
  
  def complete: Future[Unit]
  
  /** Called each time an Accept message is received from a new peer
   *  
   *  Finalizers are created immediately upon reaching the threshold commit requirement. Finalizers
   *  that depend on knowing which peers have successfully processed the transaction (as indicated by
   *  them sending Accepted messages) may delay action for some time to receive additional notifications
   *  via this callback. 
   */
  def updateAcceptedPeers(acceptedPeers: Set[DataStoreID]): Unit
  
  /** Called when a TxFinalized message is received. */ 
  def cancel(): Unit
  
}

object TransactionFinalizer {
  trait Factory {
    def create(txd: TransactionDescription, acceptedPeers: Set[DataStoreID], messenger: StoreSideTransactionMessenger): TransactionFinalizer
  }
}