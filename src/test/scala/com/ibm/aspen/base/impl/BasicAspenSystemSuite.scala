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

class BasicAspenSystemSuite extends AsyncFunSuite with Matchers {
  import Bootstrap._
  
  override implicit val executionContext = ExecutionContext.Implicits.global
  
  test("Test Bootstrapping Logic") {
    val ms = new MockSystem
    val noRetry = new NoRetry
    
    val r = Await.result(ms.basicAspenSystem.radicle, 100 milliseconds)
    
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
  }
  
  test("Test Finalization Handler Logic") {
    val ms = new MockSystem(9999)
    val noRetry = new NoRetry
    
    def allocObj(r: Radicle): Future[ObjectPointer] = {
      implicit val tx = ms.basicAspenSystem.newTransaction()
      val ffp = ms.basicAspenSystem.lowLevelAllocateObject(r.systemTreeDefinitionPointer, ObjectRevision(0), BootstrapStoragePoolUUID,
                                                   None, ms.bootstrapPoolIDA, DataBuffer(ByteBuffer.allocate(5)))
      for {
        fp <- ffp
        committed <- tx.commit()
      } yield fp 
    }
    
    var allocTreeEntryCount = 0
    
    def visitEntry(key: Array[Byte], value: Array[Byte]): Unit = synchronized {
      allocTreeEntryCount += 1
    }
    
    for {
      r <- ms.basicAspenSystem.radicle
      fp1 <- allocObj(r)
      faComplete1 <- ms.allFinalizationActionsCompleted
      fp2 <- allocObj(r)
      faComplete2 <- ms.allFinalizationActionsCompleted
      sp <- ms.basicAspenSystem.getStoragePool(Bootstrap.BootstrapStoragePoolUUID)
      spAllocTreeDef <- sp.getAllocationTreeDefinitionPointer(noRetry)
      atree <- ms.kvTreeFactory.createTree(spAllocTreeDef)
      visitComplete <- atree.visitRange(new Array[Byte](0), None, visitEntry)
    } yield {
      allocTreeEntryCount should be (BootstrapAllocatedObjectCount + 2)
    }
  }
}