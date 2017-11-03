package com.ibm.aspen.base.impl

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import org.scalatest._
import scala.language.postfixOps
import com.ibm.aspen.base.NoRetry
import com.ibm.aspen.core.objects.ObjectRevision
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectPointer
import java.io.File
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.TestNetwork
import com.ibm.aspen.core.ida.Replication

object BasicIntegrationSuite {
  
}

class BasicIntegrationSuite  extends AsyncFunSuite with Matchers with BeforeAndAfter {
  import BasicIntegrationSuite._
  import Bootstrap._
  
  var tdir:File = _
  var tdirMgr: TempDirManager = _
  var stores: List[RocksDBDataStore] = Nil
  
  def newStore(storeId: DataStoreID): RocksDBDataStore = {
    val tpath = new File(tdir, s"dbdir_${storeId.poolIndex}").getAbsolutePath
    val db = new RocksDBDataStore(storeId, tpath)(ExecutionContext.Implicits.global)
    stores = db :: stores
    db
  }
  
  before {
    tdirMgr = new TempDirManager
    tdir = tdirMgr.tdir
  }

  after {
    stores.foreach(_.close())
    stores = Nil
    
    tdirMgr.delete()
  }
  
  test("Test Bootstrapping Logic") {
    val net = new TestNetwork
    val noRetry = new NoRetry
    val bootstrapPoolIDA = new Replication(3,2)
    val store0 = newStore(DataStoreID(BootstrapStoragePoolUUID, 0))
    val store1 = newStore(DataStoreID(BootstrapStoragePoolUUID, 1))
    val store2 = newStore(DataStoreID(BootstrapStoragePoolUUID, 2))
    
    // Bootstrap system
    val r = Await.result(Bootstrap.initializeNewSystem(List(store0, store1, store2), bootstrapPoolIDA), 500 milliseconds)
    
    
    
    //*** HERE ***
    /*
    val osd = ms.storage.read(r.systemTreeDefinitionPointer).get
    
    val sp = Await.result(ms.basicAspenSystem.getStoragePool(Bootstrap.BootstrapStoragePoolUUID), 100 milliseconds)
    val spAllocTreeDef = Await.result(sp.getAllocationTreeDefinitionPointer(noRetry), 100 milliseconds)
    val atree = Await.result(ms.kvTreeFactory.createTree(spAllocTreeDef), 100 milliseconds)
    
    var allocTreeEntryCount = 0
    
    def visitEntry(key: Array[Byte], value: Array[Byte]): Unit = synchronized {
      allocTreeEntryCount += 1
    }
    
    Await.result(atree.visitRange(new Array[Byte](0), None, visitEntry), 100 milliseconds)
    
    allocTreeEntryCount should be (BootstrapAllocatedObjectCount)
    * 
    */
    0 should be(0)
  }
}