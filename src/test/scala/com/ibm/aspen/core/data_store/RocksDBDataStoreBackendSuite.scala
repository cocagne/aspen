package com.ibm.aspen.core.data_store

import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest.BeforeAndAfter
import java.io.File
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.impl.TempDirManager

class RocksDBDataStoreBackendSuite extends DataStoreSuite with BeforeAndAfter {
  var db:RocksDBDataStoreBackend = null
  
  def newStore: DataStore = {
    if (db != null)
      Await.result(db.close(), Duration(10000, MILLISECONDS))
    val tpath = new File(tdir, "dbdir").getAbsolutePath
    
    db = new RocksDBDataStoreBackend(tpath)(ExecutionContext.Implicits.global)
    
    new DataStoreFrontend(DataStoreSuite.storeId, db, Nil, Nil)
  }
  
  var tdir:File = _
  var tdirMgr: TempDirManager = _
  
  before {
    tdirMgr = new TempDirManager
    tdir = tdirMgr.tdir
  }

  after {
    Await.result(db.close(), Duration(10000, MILLISECONDS))
    db = null
    
    tdirMgr.delete()
  }
}