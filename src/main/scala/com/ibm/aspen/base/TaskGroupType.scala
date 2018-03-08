package com.ibm.aspen.base

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.DataObjectPointer

trait TaskGroupType extends TypeFactory {
  val typeUUID: UUID
  
  def createTaskGroup(
      system: AspenSystem,
      taskGroupInstanceUUID: UUID,
      taskGroupDefinitionPointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[TaskGroup]
  
  def createTaskGroupExecutor(
      system: AspenSystem,
      taskGroupInstanceUUID: UUID,
      taskGroupDefinitionPointer: DataObjectPointer,
      taskRegistry: TypeRegistry[TaskType],
      retryStrategy: RetryStrategy,
      taskObjectAllocater: ObjectAllocater)(implicit ec: ExecutionContext): Future[TaskGroupExecutor]
}