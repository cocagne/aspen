package com.ibm.aspen.base.impl

import com.ibm.aspen.core.data_store.DataStoreSuite
import org.scalatest.BeforeAndAfter
import java.io.File
import com.ibm.aspen.core.data_store.DataStore
import scala.concurrent.ExecutionContext

// *** NOTE: Tests are inherited from DataStoreSuite
class RocksDBDataStoreSuite extends DataStoreSuite with BeforeAndAfter {
  var db:RocksDBDataStore = null
  
  def newStore: DataStore = {
    if (db != null)
      db.close()
    val tpath = new File(tdir, "dbdir").getAbsolutePath
    db = new RocksDBDataStore(DataStoreSuite.storeId, tpath)(ExecutionContext.Implicits.global)
    db.initialize(Nil)
    db
  }
  
  var tdir:File = _
  var tdirMgr: TempDirManager = _
  
  before {
    tdirMgr = new TempDirManager
    tdir = tdirMgr.tdir
  }

  after {
    db.close()
    db = null
    
    tdirMgr.delete()
  }
}