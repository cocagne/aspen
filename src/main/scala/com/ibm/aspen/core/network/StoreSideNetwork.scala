package com.ibm.aspen.core.network

import com.ibm.aspen.core.data_store.DataStoreID

trait StoreSideNetwork {
  val readHandler: StoreSideReadHandler
  val allocationHandler: StoreSideAllocationHandler
  val transactionHandler: StoreSideTransactionHandler
  
  /** Called when a storage node begins hosting a data store */
  def registerHostedStore(storeId: DataStoreID): Unit
  
  /** Called when a storage node stops hosting a data store */
  def unregisterHostedStore(storeId: DataStoreID): Unit
}