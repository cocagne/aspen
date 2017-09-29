package com.ibm.aspen.core.transaction


import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.data_store.DataStoreID
import scala.concurrent.Future

trait TransactionFinalizer {
  
  def complete: Future[Unit]
  
  /** Called when a TxFinalized message is received. */ 
  def cancel(): Unit
  
}

object TransactionFinalizer {
  trait Factory {
    def create(txd: TransactionDescription, acceptedPeers: Set[DataStoreID], messenger: StoreSideTransactionMessenger): TransactionFinalizer
  }
}