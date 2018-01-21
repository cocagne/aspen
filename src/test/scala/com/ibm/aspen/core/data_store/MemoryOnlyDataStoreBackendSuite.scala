package com.ibm.aspen.core.data_store

import scala.concurrent.ExecutionContext
class MemoryOnlyDataStoreBackendSuite extends DataStoreSuite {
  
  def newStore: DataStore = new DataStoreFrontend(DataStoreSuite.storeId, 
      new MemoryOnlyDataStoreBackend()(ExecutionContext.Implicits.global), Nil, Nil)
}