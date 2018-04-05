package com.ibm.aspen.base.task

import java.util.UUID
import scala.concurrent.Future
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.ObjectRevision

object DurableTask {
  val TaskTypeKey = Key(0) // Corresponds to TaskType UUID
  
  val IdleTaskType = new UUID(0,0)
}

trait DurableTask {
  
  val taskPointer: DurableTaskPointer
  
  /** Future is to the revision of the object when the task completes. 
   *  
   *  This is intended to facilitate re-use of existing Task objects by allowing the TaskGroupExecutor
   *  to learn the revision of the completed task without having to first read the state of the object
   */
  def completed: Future[(ObjectRevision, Option[AnyRef])]
  
  def resume(): Unit
  
  def suspend(): Unit
}