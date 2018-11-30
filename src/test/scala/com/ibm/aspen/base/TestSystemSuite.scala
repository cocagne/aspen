package com.ibm.aspen.base

import org.scalatest._

import scala.concurrent.{Await, Future}
import com.ibm.aspen.base.impl.BasicAspenSystem
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import java.util.concurrent.TimeoutException

import com.ibm.aspen.base.impl.Bootstrap
import com.ibm.aspen.core.allocation.ObjectAllocationRevisionGuard
import com.ibm.aspen.core.objects.keyvalue.Insert

import scala.concurrent.duration._

class TestSystemSuite extends AsyncFunSuite with Matchers with BeforeAndAfter {
  var ts: TestSystem = null
  var sys: BasicAspenSystem = null
  var testName: String = "NO_TEST"

  import Bootstrap._
  /*
  before {
    ts = createNewTestSystem()
    sys = ts.aspenSystem
  }
*/
  after {
    try {
      Await.result(ts.waitForTransactionsComplete(), Duration(5000, MILLISECONDS))
    } catch {
      case e: TimeoutException =>
        println(s"TEST LEFT TRANSACTIONS UNFINISHED: $testName")
        ts.printTransactionStatus()
        throw e
    }
    ts.shutdown()
    ts = null
    sys = null
    testName = "NO_TEST"
  }

  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    testName = test.name
    ts = createNewTestSystem()
    sys = ts.aspenSystem
    sys.setSystemAttribute("unittest.name", test.name)
    test()
  }
  
  def createNewTestSystem(): TestSystem = new TestSystem()
  
  def waitForTransactionsComplete(): Future[Unit] = ts.waitForTransactionsComplete()
  
  def waitForIt(errMsg: String)(fn: => Boolean): Future[Unit] = Future {
    
    var count = 0
    while (!fn && count < 500) {
      count += 1
      Thread.sleep(5) 
    }
        
    if (count >= 500)
      throw new Exception(errMsg)
  }
  
  def kvalloc(contents: List[(Key,Array[Byte])] = Nil): Future[KeyValueObjectPointer] = {
    
    implicit val tx = sys.newTransaction()

    var ops = List[KeyValueOperation]()
    
    contents.foreach { t => ops = Insert(t._1, t._2) :: ops }
    
    for {
      r <- sys.readObject(sys.radiclePointer)
      
      // give transaction something to do
      meh = tx.bumpVersion(sys.radiclePointer, r.revision)
      
      kvp <- sys.lowLevelAllocateKeyValueObject(
        ObjectAllocationRevisionGuard(sys.radiclePointer,
            ObjectRevision(UUID.randomUUID())),
            BootstrapStoragePoolUUID, 
            None,
            TestSystem.DefaultIDA, 
            ops)
            
      done <- tx.commit()
    } yield kvp
  }
}