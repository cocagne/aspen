package com.ibm.aspen.core.data_store

import java.io.File
import scala.concurrent.ExecutionContext

class RocksDBDataStoreBackendSuite extends DataStoreBackendSuite {
  override def preTest(): Unit = {
    val tpath = new File(tdir, "dbdir").getAbsolutePath
    
    backend = new RocksDBDataStoreBackend(tpath)(ExecutionContext.global)
  }
}