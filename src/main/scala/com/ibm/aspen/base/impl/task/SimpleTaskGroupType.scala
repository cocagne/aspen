package com.ibm.aspen.base.impl.task

import com.ibm.aspen.base.TaskGroupType
import java.util.UUID
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.ObjectAllocater
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.TaskGroupExecutor
import com.ibm.aspen.base.TaskGroup
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.base.TypeRegistry
import com.ibm.aspen.base.TaskType

object SimpleTaskGroupType extends TaskGroupType {
  val typeUUID: UUID = UUID.fromString("ffb56270-270c-44f0-9cb6-0bf8506469a3")
  
  def createNewTaskGroup(): DataBuffer = TaskCodec.encodeTaskGroupDefinition(Nil)
  
  def createTaskGroup(
      system: AspenSystem,
      taskGroupInstanceUUID: UUID,
      taskGroupDefinitionPointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[TaskGroup] = {
    Future.successful(new SimpleTaskGroup(system, taskGroupInstanceUUID, taskGroupDefinitionPointer))
  }
  
  def createTaskGroupExecutor(
      system: AspenSystem,
      taskGroupInstanceUUID: UUID,
      taskGroupDefinitionPointer: DataObjectPointer,
      taskRegistry: TypeRegistry[TaskType],
      retryStrategy: RetryStrategy,
      taskObjectAllocater: ObjectAllocater)(implicit ec: ExecutionContext): Future[TaskGroupExecutor] = {
    Future.successful(new SimpleTaskGroupExecutor(system, taskGroupInstanceUUID, taskGroupDefinitionPointer, taskRegistry, retryStrategy, taskObjectAllocater))
  }
  
  private[task] case class TaskGroupState(tasks: List[TaskDefinition], revision: ObjectRevision)
  
  private[task] def loadGroupState(
    system: AspenSystem, 
    groupDefinitionPointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[TaskGroupState] = {
    
    system.readObject(groupDefinitionPointer).map(osd =>TaskGroupState(TaskCodec.decodeTaskGroupDefinition(osd.data), osd.revision)) 
  }
  
}