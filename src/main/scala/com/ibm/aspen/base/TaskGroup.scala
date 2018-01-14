package com.ibm.aspen.base

import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.base.impl.task.TaskCreationFinalizationAction
import com.ibm.aspen.core.objects.DataObjectPointer

/** Client-side representation of a remotely-executed TaskGroup
 *  
 *  Tasks are inserted into groups by way of Finalization Actions rather than direct method invocations. The
 *  reason for this is to ensure that the addition eventually occurs even in the presence of failures (which is 
 *  why Finalization Actions exist) and to ensure that it happens exactly once. Typicially Finalization Actions
 *  only provide at-least-once semantics but in this case the addTask() method called by the Finalization Action
 *  includes a revision-checked version bump to the task definition object, thereby ensuring that the addition
 *  can occur only one time. The addTask method returns successfully if the addition transaction fails due to the
 *  revision being anything other than the expected value so the revision must not be modified by an outside
 *  source until after the task is successfully added... which basically means don't modify the object outside
 *  of the Task instance that will eventually be created to execute the task.
 */
trait TaskGroup {
  val taskGroupInstanceUUID: UUID
  val taskGroupType: TaskGroupType
  
  /** The future returns upon the successful insertion of the task into the group or fails with a
   *  non-recoverable error.
   *  
   *  This method should be used by Finalization Actions only  
   */
  protected[base] def addTask(taskTypeUUID: UUID, taskUUID: UUID, taskDefinitionPointer: DataObjectPointer, requiredRevision: ObjectRevision): Future[Unit]
  
  /** This method is called during Task creation to add a Transaction Finalization Action that ensures the
   *  newly-created transaction is eventually inserted into the specified group.  The addition to the group occurs atomically and includes a version bump of the taskDefinitionObject.
   */
  protected[base] def insertAddToGroupFinalizationAction(
      taskType: TaskType, 
      taskUUID: UUID, 
      taskDefinitionObject: DataObjectPointer, 
      taskDefinitionRevision: ObjectRevision)(implicit t: Transaction): Unit = {
    TaskCreationFinalizationAction.addToTaskGroup(t, taskGroupInstanceUUID, taskType.taskTypeUUID, 
        taskUUID, taskDefinitionObject, taskDefinitionRevision)
  }
}