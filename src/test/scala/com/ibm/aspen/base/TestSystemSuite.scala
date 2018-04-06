package com.ibm.aspen.base

import org.scalatest._
import scala.concurrent.Future
import com.ibm.aspen.base.impl.BasicAspenSystem
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.base.impl.Bootstrap
import com.ibm.aspen.core.objects.keyvalue.Insert

class TestSystemSuite extends AsyncFunSuite with Matchers with BeforeAndAfter {
  var ts: TestSystem = null
  var sys: BasicAspenSystem = null
  
  import Bootstrap._
  
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
    
    if (count >= 100) 
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
  
  def kvalloc(contents: List[(Key,Array[Byte])] = Nil): Future[KeyValueObjectPointer] = {
    
    implicit val tx = sys.newTransaction()

    var ops = List[KeyValueOperation]()
    
    contents.foreach { t => ops = Insert(t._1, t._2, tx.timestamp()) :: ops }
    
    for {
      r <- sys.readObject(sys.radiclePointer)
      
      // give transaction something to do
      meh = tx.bumpVersion(sys.radiclePointer, r.revision)
      
      kvp <- sys.lowLevelAllocateKeyValueObject(
            sys.radiclePointer, 
            ObjectRevision(UUID.randomUUID()), 
            BootstrapStoragePoolUUID, 
            None,
            TestSystem.DefaultIDA, 
            ops, 
            None)
            
      done <- tx.commit()
    } yield kvp
  }
}