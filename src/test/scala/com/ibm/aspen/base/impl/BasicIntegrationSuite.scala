package com.ibm.aspen.base.impl

import scala.concurrent._
import scala.concurrent.duration._
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
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.network.ClientID
import java.util.UUID
import com.ibm.aspen.core.read.BaseReadDriver
import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.core.allocation.BaseAllocationDriver
import com.ibm.aspen.core.transaction.TransactionDriver
import com.ibm.aspen.core.data_store.DataStore
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.read.TriggeredReread
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.base.TestSystem
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.data_store.DataStoreFrontend
import com.ibm.aspen.core.data_store.RocksDBDataStoreBackend
import com.ibm.aspen.core.data_store.MemoryOnlyDataStoreBackend
import com.ibm.aspen.core.crl.MemoryOnlyCRL
import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.base.TestSystemSuite
import com.ibm.aspen.base.tieredlist.KeyValueListPointer
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.Key


class BasicIntegrationSuite extends TestSystemSuite { 
  import Bootstrap._
  
  test("Test Bootstrapping Logic") { 

    var allocTreeEntryCount = 0
    
    def visitEntry(value: Value): Unit = synchronized {
      allocTreeEntryCount += 1
    }
    
    for {
      sp <- sys.getStoragePool(Bootstrap.BootstrapStoragePoolUUID)
      
      atree <- sp.getAllocationTree(TestSystem.NoRetry)
    
      _ <- atree.visitRange(Key.AbsoluteMinimum, None, visitEntry)
      
    } yield {
      allocTreeEntryCount should be (BootstrapAllocatedObjectCount)
    }
  }
  
  test("Test Allocation & Finalization") {
    
    var allocCount = 0
    
    def allocObj(r: KeyValueObjectState): Future[ObjectPointer] = {
      implicit val tx = sys.newTransaction()
      val d = DataBuffer(ByteBuffer.allocate(5))
      val ffp = sys.lowLevelAllocateDataObject(r.pointer, ObjectRevision.Null, BootstrapStoragePoolUUID,
                                    None, TestSystem.DefaultIDA, d)
      allocCount += 1
      
      for {
        fp <- ffp
        // Need to give the transaction something to do. Modify refcount instead of data so we don't accidentally corrupt anything
        y = tx.setRefcount(r.pointer, ObjectRefcount(0,allocCount), ObjectRefcount(0,allocCount + 1))
        committed <- tx.commit()
      } yield {
        fp 
      }
    }

    var allocTreeEntryCount = 0
    
    def visitEntry(value: Value): Unit = synchronized {
      allocTreeEntryCount += 1
    }
    
    for {
      r <- sys.radicle
      fp1 <- allocObj(r)
      faComplete1 <- waitForTransactionsComplete()
      fp2 <- allocObj(r)
      faComplete2 <- waitForTransactionsComplete()
      
      sp <- sys.getStoragePool(Bootstrap.BootstrapStoragePoolUUID)
      atree <- sp.getAllocationTree(TestSystem.NoRetry)
      
      visitComplete <- atree.visitRange(Key.AbsoluteMinimum, None, visitEntry)
    } yield {
      allocTreeEntryCount should be (BootstrapAllocatedObjectCount + 2)
    }
  }
}