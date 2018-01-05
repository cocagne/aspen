package com.ibm.aspen.base.impl.task

import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.FinalizationActionHandler
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.FinalizationAction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class TaskCreationFinalizationAction(
    val retryStrategy: RetryStrategy,
    val system: AspenSystem) extends FinalizationActionHandler {
  
  val supportedUUIDs: Set[UUID] = TaskCreationFinalizationAction.supportedUUIDs
  
  class AddToGroup(val fa: TaskCreationFinalizationAction.FAContent) extends FinalizationAction {
    
    def execute()(implicit ec: ExecutionContext): Future[Unit] = retryStrategy.retryUntilSuccessful {
      // TODO: Detect deleted TaskGroup and add to global "Orphaned Tasks" Group
      
      for {
        taskGroup <- system.getTaskGroup(fa.taskGroupUUID) 
        addComplete <- taskGroup.addTask(fa.taskTypeUUID, fa.taskUUID, fa.taskObject, fa.taskRevision) 
      } yield {
        ()
      }
    }
  
    def completionDetected(): Unit = ()
  }
  
  def createAction(
      finalizationActionUUID: UUID, 
      serializedActionData: Array[Byte]): Option[FinalizationAction] = finalizationActionUUID match {
    
    case TaskCreationFinalizationAction.TaskCreationFinalizationActionUUID =>
      val fa = TaskCodec.decodeTaskCreationFinalizationAction(serializedActionData)
      
      Some(new AddToGroup(fa))
      
    case _ => None
  }
  
}

object TaskCreationFinalizationAction {
  
  val TaskCreationFinalizationActionUUID = UUID.fromString("5f1c51b5-77ca-4bd5-9823-bd774cf9f432")
  
  val supportedUUIDs: Set[UUID] = Set(TaskCreationFinalizationActionUUID)
  
  case class FAContent(taskGroupUUID: UUID, taskTypeUUID: UUID, taskUUID: UUID, taskObject:ObjectPointer, taskRevision: ObjectRevision)
  
  def addToTaskGroup(transaction: Transaction, taskGroupUUID: UUID, taskTypeUUID: UUID, taskUUID: UUID, taskObject:ObjectPointer, taskRevision: ObjectRevision): Unit = {
    val fac = FAContent(taskGroupUUID, taskTypeUUID, taskUUID, taskObject, taskRevision)
    
    val serializedContent = TaskCodec.encodeTaskCreationFinalizationAction(fac)
    
    transaction.addFinalizationAction(TaskCreationFinalizationActionUUID, serializedContent)
  }
}