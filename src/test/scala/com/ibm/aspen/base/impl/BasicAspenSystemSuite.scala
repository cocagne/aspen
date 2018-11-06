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
import com.ibm.aspen.core.allocation.ObjectAllocationRevisionGuard
import com.ibm.aspen.core.objects.keyvalue.{Insert, Key, Value}
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectState

class BasicAspenSystemSuite extends TestSystemSuite {
  import Bootstrap._
  
  override implicit val executionContext = ExecutionContext.Implicits.global
  
  test("Test Update Object During Allocation") {
    
    val noRetry = new NoRetry
    
    val update = Array[Byte](1,2,3)

    implicit val tx = sys.newTransaction()
    
    val d = DataBuffer(ByteBuffer.allocate(0))
    val d2  = DataBuffer(ByteBuffer.wrap(update))
    
    for {
      radicle <- sys.radicle
      
      fp <- sys.lowLevelAllocateDataObject(ObjectAllocationRevisionGuard(radicle.pointer, ObjectRevision.Null), BootstrapStoragePoolUUID,
                                    None, TestSystem.DefaultIDA, d)
      _=tx.overwrite(fp, tx.txRevision, d2)
      
      committed <- tx.commit()
      
      o <- sys.readObject(fp)
    } yield {
      o.data should not be (d)
      o.data should be (d2)
    }
  }

  test("Test 100 Sequential KeyValue Object Transactions") {

    val key = Key(1)

    def rupdate(ptr: KeyValueObjectPointer, revision: ObjectRevision, count: Int): Future[Unit] =  {
      if (count == 100)
        Future.unit
      else {
        val tx = sys.newTransaction()
        val arr = Array[Byte](count.asInstanceOf[Byte])
        val ops = Insert(key, arr) :: Nil
        tx.update(ptr, Some(revision), Nil, ops)
        tx.commit()
        tx.result.flatMap(_ => rupdate(ptr, tx.txRevision, count + 1))
      }
    }

    for {
      ptr <- kvalloc(Nil)
      kvos <- sys.readObject(ptr)
      _ <- rupdate(ptr, kvos.revision, 0)
      kvos <- sys.readObject(ptr)
    } yield {
      kvos.contents(key).value(0) should be (99)
    }
  }

  test("Test Finalization Handler Logic") {
    
    val noRetry = new NoRetry
    
    def allocObj(radicle: KeyValueObjectState): Future[ObjectPointer] = {
      implicit val tx = sys.newTransaction()
      val d = DataBuffer(ByteBuffer.allocate(5))
      val ffp = sys.lowLevelAllocateDataObject(ObjectAllocationRevisionGuard(radicle.pointer, ObjectRevision.Null), BootstrapStoragePoolUUID,
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
      
      visitComplete <- atree.visitRange(Key.AbsoluteMinimum, None, visitEntry)
    } yield {
      allocTreeEntryCount should be (BootstrapAllocatedObjectCount + 2)
    }
  }
}