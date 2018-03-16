package com.ibm.aspen.base.task

import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.KeyValueObjectState
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.util.uuid2byte
import com.ibm.aspen.base.TestSystem
import com.ibm.aspen.base.TestSystemSuite
import org.scalactic.source.Position.apply
import com.ibm.aspen.base.impl.Bootstrap

class SteppedTaskSuite  extends TestSystemSuite { 
  import Bootstrap._
  
  def alloc(taskType: Option[UUID], contents: List[(Key,Array[Byte])] = Nil): Future[KeyValueObjectPointer] = {
    
    implicit val tx = sys.newTransaction()

    var ops = List[KeyValueOperation]()
    
    taskType.foreach { uuid => ops = Insert(DurableTask.TaskTypeKey, uuid2byte(uuid), tx.timestamp()) :: ops }
    
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
  
  test("Test Stepped Task") { 
    
    
    val tkey = Key(Array[Byte](2))
    
    class TS(target: KeyValueObjectPointer, initialState: KeyValueObjectState) 
       extends SteppedDurableTask(new DurableTaskPointer(initialState.pointer), sys, initialState.revision, initialState.contents) {
      
      var steps: List[Int] = Nil
      
      def suspend(): Unit = ()
      
      def beginStep(): Unit = synchronized {
        
        steps = step :: steps
        
        implicit val tx = sys.newTransaction()
        
        if (step == 3) {
          for {
            o <- sys.readObject(target)
            _ = tx.bumpVersion(target, o.revision)
            _ = completeTask(tx)
            done <- tx.commit()
          } yield ()
        } else {
          for {
            o <- sys.readObject(target)
            _ = tx.bumpVersion(target, o.revision)
            _ = completeStep(tx, List((tkey ->SteppedDurableTask.encodeStep(step+1))))
            done <- tx.commit()
          } yield ()
        }
      }
    }
         
    val TType = new UUID(0,1)

    for {
      target <- alloc(None, Nil)
      taskPtr <- alloc(Some(TType), Nil)
      
      initialState <- sys.readObject(taskPtr)
      
      ts = new TS(target, initialState)
      _ = ts.resume()
      
      taskComplete <- ts.completed
      
      // --- Test Mid-Process Resumed Task ---
      
      taskPtr2 <- alloc(Some(TType), List((SteppedDurableTask.StepKey -> SteppedDurableTask.encodeStep(2))))
      initialState2 <- sys.readObject(taskPtr2)
      
      ts2 = new TS(target, initialState2)
      _ = ts2.resume()
      
      taskComplete <- ts2.completed
      
      
    } yield {
      ts.steps.reverse should be (List(0, 1, 2, 3))
      ts2.steps.reverse should be (List(2, 3))
    }
  }
}