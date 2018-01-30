package com.ibm.aspen.base

import org.scalatest._
import scala.concurrent.Future
import com.ibm.aspen.base.impl.BasicAspenSystem

class TestSystemSuite extends AsyncFunSuite with Matchers with BeforeAndAfter {
  var ts: TestSystem = null
  var sys: BasicAspenSystem = null
  
  before {
    ts = createNewTestSystem()
    sys = ts.aspenSystem
  }

  after {
    ts.shutdown()
    ts = null
    sys = null
  }
  
  def createNewTestSystem(): TestSystem = new TestSystem()
  
  def waitForTransactionsComplete(): Future[Unit] = Future {

    var count = 0
    
    while (!ts.sn0.allTransactionsComplete && count < 100) {
      count += 1
      Thread.sleep(5)
    }

    if (count > 100)
      throw new Exception("Finalization Actions Timed Out")
  }
  
  def waitForIt(errMsg: String)(fn: => Boolean): Future[Unit] = Future {
    
    var count = 0
    while (!fn && count < 500) {
      count += 1
      Thread.sleep(5) 
    }
        
    if (count >= 500)
      throw new Exception(errMsg)
  }
}