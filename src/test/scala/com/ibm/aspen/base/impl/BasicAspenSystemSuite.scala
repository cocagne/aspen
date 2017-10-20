package com.ibm.aspen.base.impl

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import org.scalatest._
import scala.language.postfixOps
import com.ibm.aspen.base.NoRetry

class BasicAspenSystemSuite extends AsyncFunSuite with Matchers {
  test("Test Bootstrapping Logic") {
    val ms = new MockSystem
    val noRetry = new NoRetry
    
    val r = Await.result(ms.basicAspenSystem.radicle, 100 milliseconds)
    
    val osd = ms.storage.read(r.bootstrapPoolDefinitionPointer).get
    
    val sp = Await.result(ms.basicAspenSystem.getStoragePool(Bootstrap.BootstrapStoragePoolUUID), 100 milliseconds)
    val spAllocTreeDef = Await.result(sp.getAllocationTreeDefinitionPointer(noRetry), 100 milliseconds)
    val atree = Await.result(ms.kvTreeFactory.createTree(spAllocTreeDef), 100 milliseconds)
    
    var allocTreeEntryCount = 0
    
    def visitEntry(key: Array[Byte], value: Array[Byte]): Unit = synchronized {
      allocTreeEntryCount += 1
    }
    
    Await.result(atree.visitRange(new Array[Byte](0), None, visitEntry), 100 milliseconds)
    
    allocTreeEntryCount should be (5)
  }
}