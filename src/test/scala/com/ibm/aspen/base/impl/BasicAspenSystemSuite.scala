package com.ibm.aspen.base.impl

import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest._
import scala.language.postfixOps
import com.ibm.aspen.base.NoRetry
import com.ibm.aspen.core.objects.ObjectRevision
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.DataBuffer
import java.util.UUID
import com.ibm.aspen.base.TestSystemSuite
import com.ibm.aspen.base.TestSystem
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.base.tieredlist.KeyValueListPointer
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectState

class BasicAspenSystemSuite extends TestSystemSuite {
  import Bootstrap._
  
  override implicit val executionContext = ExecutionContext.Implicits.global
  
  test("Test Finalization Handler Logic") {
    
    val noRetry = new NoRetry
    
    def allocObj(radicle: KeyValueObjectState): Future[ObjectPointer] = {
      implicit val tx = sys.newTransaction()
      val d = DataBuffer(ByteBuffer.allocate(5))
      val ffp = sys.lowLevelAllocateDataObject(radicle.pointer, ObjectRevision.Null, BootstrapStoragePoolUUID,
                                    None, TestSystem.DefaultIDA, d)
      
      for {
        fp <- ffp
        // Need to give the transaction something to do. Modify refcount instead of data so we don't accidentally corrupt anything
        y = tx.setRefcount(radicle.pointer, ObjectRefcount(0,1), ObjectRefcount(0,1))
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
      
      visitComplete <- atree.visitRange(KeyValueListPointer.AbsoluteMinimum, None, visitEntry)
    } yield {
      allocTreeEntryCount should be (BootstrapAllocatedObjectCount + 2)
    }
  }
}