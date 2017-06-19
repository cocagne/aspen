package com.ibm.aspen.core.data_store

class MemoryOnlyDataStoreSuite extends DataStoreSuite {
  def newStore: DataStore = new MemoryOnlyDataStore(DataStoreSuite.storeId)
}