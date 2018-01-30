package com.ibm.aspen.base.impl.task

import org.scalatest.AsyncFunSuite
import org.scalatest.Matchers
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.base.TestSystem
import com.ibm.aspen.base.TaskType
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.TaskGroup
import com.ibm.aspen.core.objects.ObjectState
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.Task
import scala.concurrent.Promise
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.base.Transaction
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.base.TaskTypeRegistry
import com.ibm.aspen.base.impl.BasicAspenSystem
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.DataObjectState

class SimpleTaskSuite extends AsyncFunSuite with Matchers {

  def waitForIt(errMsg: String)(fn: => Boolean): Future[Unit] = Future {
    
    var count = 0
    while (!fn && count < 500) {
      count += 1
      Thread.sleep(5) 
    }
        
    if (count >= 500)
      throw new Exception(errMsg)
  }
  
  test("Test Tasks") {
    
    object TestTask extends TaskType with TaskTypeRegistry {
      val taskTypeUUID: UUID = new UUID(0,100)
      
      private var executed = Map[UUID, Int]()
      
      var sys:BasicAspenSystem = null
      
      def add(uuid: UUID, num: Int) = synchronized { executed +=  (uuid -> num) }
      
      def execMap = synchronized { executed }
      
      def createTask(
          group: TaskGroup,
          allocater: ObjectAllocater,
          taskUUID: UUID,
          taskNum: Int)(implicit t: Transaction): Future[ObjectPointer] = {
        // Don't need a real object for the "allocating object" this is just for allocation Tx error recovery
        val allocatingObject = DataObjectPointer(new UUID(0,0), new UUID(0,0), None, Replication(3,2), new Array[StorePointer](0))
        val allocatingObjectRevision = ObjectRevision.Null
        val bb = ByteBuffer.allocate(4)
        bb.putInt(taskNum)
        bb.position(0)
        val initialState = DataBuffer(bb)
        
        // Need to give the transaction something to do. Modify refcount instead of data so we don't accidentally corrupt anything
        t.setRefcount(sys.radiclePointer, ObjectRefcount(0,1), ObjectRefcount(0,1))
        
        createTaskObject(group, allocater, allocatingObject, allocatingObjectRevision, taskUUID, initialState)
      }
      def createTaskExecutor(
          system: AspenSystem,
          taskUUID: UUID, 
          taskStatePointer: DataObjectPointer,
          taskState: DataObjectState)(implicit ec: ExecutionContext): Future[Task] = {
        Future.successful(new TestTask(system, taskUUID, taskStatePointer, taskState))
      }
      
      override def getTaskType(taskTypeUUID: UUID): Option[TaskType] = {
        if (taskTypeUUID == this.taskTypeUUID) Some(this) else None
      }
    }
    
    class TestTask(
        val system: AspenSystem,
        val taskUUID: UUID, 
        val taskStatePointer: ObjectPointer,
        initialState: DataObjectState
        ) extends Task {
      
      val taskType: TaskType = TestTask
      
      val p = Promise[Unit]()
      
      def complete: Future[Unit] = p.future
      
      def resume(): Unit = {
        val taskNum = initialState.data.getInt(0)
        TestTask.add(taskUUID, taskNum)
        p.success(())
      }
      
      def pause(): Unit = {}
    }
    
    val ts = new TestSystem(userTaskTypeRegistry=Some(TestTask))
    
    val sys = ts.aspenSystem
    
    TestTask.sys = sys
    
    val groupUUID = new UUID(1,1)
    val task0 = new UUID(2,2)
    val num0 = 2
    val task1 = new UUID(3,3)
    val num1 = 3
    
    val fresult = for {
      r <- sys.radicle
      stg <- sys.createTaskGroup(groupUUID, SimpleTaskGroupType.groupTypeUUID, SimpleTaskGroupType.createNewTaskGroup())
      
      tx0 = sys.newTransaction()
      tx0Ready <- TestTask.createTask(stg, sys.bootstrapPoolAllocater, task0, num0)(tx0)
      tx0Done <- tx0.commit()
      
      tx1 = sys.newTransaction()
      tx1Ready <- TestTask.createTask(stg, sys.bootstrapPoolAllocater, task1, num1)(tx1)
      tx1Done <- tx1.commit()
      
      executor <- sys.createTaskGroupExecutor(groupUUID)
      
      initDone <- executor.initialized
      
      done <- waitForIt("Tasks failed to complete") {
        TestTask.execMap.size == 2
      }
    } yield {
      TestTask.execMap should be(Map( (task0 -> num0), (task1 -> num1) ))
    } 
    
    fresult andThen { 
      case _ => ts.shutdown()
    }
  }
  
  test("TaskGroupDefinition encoding") {
    val uuid0 = new UUID(0,0)
    val uuid1 = new UUID(0,1)
    val poolUUID = new UUID(0,2)
    val taskType = new UUID(0,3)
    
    val obj0 = DataObjectPointer(uuid0, poolUUID, None, Replication(3,2), new Array[StorePointer](0))
    val obj1 = DataObjectPointer(uuid1, poolUUID, None, Replication(3,2), new Array[StorePointer](0))
    
    val tasks = List(TaskDefinition(taskType, uuid0, obj0), TaskDefinition(taskType, uuid1, obj1))
    
    val db = TaskCodec.encodeTaskGroupDefinition(tasks)
    
    val decodedTasks = TaskCodec.decodeTaskGroupDefinition(db)
    
    decodedTasks should be (tasks)
  }
}