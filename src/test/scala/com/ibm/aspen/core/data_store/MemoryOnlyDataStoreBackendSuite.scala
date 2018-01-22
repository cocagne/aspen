package com.ibm.aspen.core.data_store

class MemoryOnlyDataStoreBackendSuite extends DataStoreBackendSuite {
  override def preTest(): Unit = {
    backend = new MemoryOnlyDataStoreBackend()
  }
}