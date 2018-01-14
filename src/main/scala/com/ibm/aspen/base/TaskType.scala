package com.ibm.aspen.base

import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.base.impl.task.TaskCreationFinalizationAction
import com.ibm.aspen.core.objects.DataObjectPointer

trait TaskType {
  
  val taskTypeUUID: UUID
  
  protected def createTaskObject(
      group: TaskGroup,
      allocater: ObjectAllocater,
      allocatingObject: DataObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      taskUUID: UUID,
      initialState: DataBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer] = {
    
    allocater.allocateObject(allocatingObject, allocatingObjectRevision, initialState) map { ptr =>
      val rev = ObjectRevision(t.uuid)
      group.insertAddToGroupFinalizationAction(this, taskUUID, ptr, rev)
      ptr
    }
  }
  
  def createTaskExecutor(
      system: AspenSystem,
      taskUUID: UUID, 
      taskStatePointer: DataObjectPointer,
      taskState: ObjectStateAndData)(implicit ec: ExecutionContext): Future[Task]
  
  def createTaskExecutor(
      system: AspenSystem,
      taskUUID: UUID, 
      taskStatePointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[Task] = system.readObject(taskStatePointer) flatMap {
    taskState => createTaskExecutor(system, taskUUID, taskStatePointer, taskState)
  }
}