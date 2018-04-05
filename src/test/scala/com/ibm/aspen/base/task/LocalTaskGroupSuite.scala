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
import com.ibm.aspen.base.TypeRegistry
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.keyvalue.Value
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise

class LocalTaskGroupSuite extends TestSystemSuite { 
  import Bootstrap._
  
  def alloc(contents: List[(Key,Array[Byte])] = Nil): Future[KeyValueObjectPointer] = {
    
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
  
  test("Test LocalTask create and execute") { 
    
    val tkey = Key(10)
    val itgtKey = Key(11)
    val nextStepKey = Key(12)
    
    var steps: List[Int] = Nil
    
    case class Result(num: Int)
    
    val result = Result(123)
    
    class TS(pointer: DurableTaskPointer, rev: ObjectRevision, state: Map[Key, Value]) 
       extends SteppedDurableTask(pointer, sys, rev, state) {
      
      val target = KeyValueObjectPointer(state(tkey))
      
      def suspend(): Unit = ()
      
      def beginStep(): Unit = synchronized {
        
        steps = step :: steps
        
        implicit val tx = sys.newTransaction()
        
        if (step == 3) {
          for {
            o <- sys.readObject(target)
            _ = tx.bumpVersion(target, o.revision)
            _ = completeTask(tx, Some(result))
            done <- tx.commit()
          } yield ()
        } else {
          for {
            o <- sys.readObject(target)
            _ = tx.append(target, None, Nil, List(Insert(nextStepKey, Array[Byte](5), tx.timestamp())))
            _ = completeStep(tx, List((nextStepKey ->SteppedDurableTask.encodeStep(step+1))))
            done <- tx.commit()
          } yield ()
        }
      }
    }
    
    class TRegistry extends TypeRegistry[DurableTaskType] with DurableTaskType {
      
      def getTypeFactory(typeUUID: UUID): Option[DurableTaskType] = Some(this)
      
      val typeUUID: UUID = new UUID(5,5)
 
      def createTask(
          system: AspenSystem, 
          pointer: DurableTaskPointer, 
          revision: ObjectRevision, 
          state: Map[Key, Value])(implicit ec: ExecutionContext): DurableTask = new TS(pointer, revision, state)
    }
    
    val registry = new TRegistry
    
    ts.taskTypeRegistry = Some(registry)
         
    val TType = new UUID(0,1)

    for {
      target <- alloc()
      groupPtr <- alloc()
      groupKvos <- sys.readObject(groupPtr)
      
      taskGroup <- LocalTaskGroup.initializeNewGroup(sys, TaskGroupPointer(groupPtr), groupKvos.revision, Bootstrap.BootstrapObjectAllocaterUUID)
      
      tx = sys.newTransaction()
      _ = tx.append(target, None, Nil, List(Insert(itgtKey, Array[Byte](1), tx.timestamp())))
      
      ftaskResult <- taskGroup.prepareTask(registry, List((tkey, target.toArray)))(tx)
      
      taskCreated <- tx.commit()
      
      taskResult <- ftaskResult
      tgtkvos <- sys.readObject(target) 
    } yield {
      steps.reverse should be (List(0, 1, 2, 3))
      
      taskResult should be (Some(result))
      
      tgtkvos.contents.contains(itgtKey) should be (true)
      tgtkvos.contents.contains(nextStepKey) should be (true)
      
      java.util.Arrays.equals(tgtkvos.contents(itgtKey).value, Array[Byte](1)) should be (true)
      java.util.Arrays.equals(tgtkvos.contents(nextStepKey).value, Array[Byte](5)) should be (true)
    }
  }
  
  test("Test LocalTask resume execution") { 
    
    val tkey = Key(10)
    val itgtKey = Key(11)
    val nextStepKey = Key(12)
    
    var steps: List[Int] = Nil
    var count = 0
    val secondExecPromise = Promise[Unit]()
    var taskPtr: KeyValueObjectPointer = null
    
    class TS(pointer: DurableTaskPointer, rev: ObjectRevision, state: Map[Key, Value]) 
       extends SteppedDurableTask(pointer, sys, rev, state) {
      
      taskPtr = pointer.kvPointer
      count += 1
      
      if (count == 2) {
        secondExecPromise completeWith completed.map(_=>())
      }
      
      val target = KeyValueObjectPointer(state(tkey))
      
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
            _ = tx.append(target, None, Nil, List(Insert(nextStepKey, Array[Byte](5), tx.timestamp())))
            _ = completeStep(tx, List((nextStepKey ->SteppedDurableTask.encodeStep(step+1))))
            done <- tx.commit()
          } yield ()
        }
      }
    }
    
    class TRegistry extends TypeRegistry[DurableTaskType] with DurableTaskType {
      
      def getTypeFactory(typeUUID: UUID): Option[DurableTaskType] = Some(this)
      
      val typeUUID: UUID = new UUID(5,5)
 
      def createTask(
          system: AspenSystem, 
          pointer: DurableTaskPointer, 
          revision: ObjectRevision, 
          state: Map[Key, Value])(implicit ec: ExecutionContext): DurableTask = new TS(pointer, revision, state)
    }
    
    val registry = new TRegistry
    
    ts.taskTypeRegistry = Some(registry)
         
    val TType = new UUID(0,1)

    for {
      target <- alloc()
      groupPtr <- alloc()
      groupKvos <- sys.readObject(groupPtr)
      
      taskGroup <- LocalTaskGroup.initializeNewGroup(sys, TaskGroupPointer(groupPtr), groupKvos.revision, Bootstrap.BootstrapObjectAllocaterUUID)
      
      tx = sys.newTransaction()
      _ = tx.append(target, None, Nil, List(Insert(itgtKey, Array[Byte](1), tx.timestamp())))
      
      ftaskDone <- taskGroup.prepareTask(registry, List((tkey, target.toArray)))(tx)
      
      taskCreated <- tx.commit()
      
      taskDone <- ftaskDone
      tgtkvos <- sys.readObject(target)
      
      // overwrite task pointer with valid content
      tskKvos <- sys.readObject(taskPtr)
      tx = sys.newTransaction()
      ops = Insert(DurableTask.TaskTypeKey, uuid2byte(registry.typeUUID), tx.timestamp()) :: Insert(tkey, target.toArray, tx.timestamp()) :: Nil 
      _ = tx.overwrite(taskPtr, tskKvos.revision, Nil, ops)
      done <- tx.commit()
      
      //create new group
      groupState <- sys.readObject(groupPtr)
      taskGroup2 <- LocalTaskGroup.createGroup(sys, groupState)
      
      // Await completion of resumed task
      allDone <- secondExecPromise.future
      
    } yield {
      steps.reverse should be (List(0, 1, 2, 3, 0, 1, 2, 3))
      tgtkvos.contents.contains(itgtKey) should be (true)
      tgtkvos.contents.contains(nextStepKey) should be (true)
      
      java.util.Arrays.equals(tgtkvos.contents(itgtKey).value, Array[Byte](1)) should be (true)
      java.util.Arrays.equals(tgtkvos.contents(nextStepKey).value, Array[Byte](5)) should be (true)
    }
  }
  
}