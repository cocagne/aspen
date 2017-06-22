package com.ibm.aspen.core.transaction


import com.ibm.aspen.core.network.TransactionMessenger
import com.ibm.aspen.core.data_store.DataStoreID

trait TransactionFinalizer {
  
  /** Called when a TxFinalized message is received. */ 
  def cancel(): Unit
}

object TransactionFinalizer {
  trait Factory {
    def create(txd: TransactionDescription, acceptedPeers: Set[DataStoreID], messenger: TransactionMessenger): TransactionFinalizer
  }
}