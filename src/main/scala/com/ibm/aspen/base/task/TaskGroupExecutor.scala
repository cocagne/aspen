package com.ibm.aspen.base.task

import scala.concurrent.Future

/** Server-side TaskGroup that executes the tasks contained within the group */
trait TaskGroupExecutor extends {
  val taskGroupType: TaskGroupType
  
  val initialized: Future[Unit]
  
  /** Resumes tasks
   * 
   */
  def resume(): Unit
  
  /** Pauses all active Tasks and stops all background activity. The TaskGroup should be prepared for
   *  garbage collection when the returned future completes 
   */
  def shutdown(): Future[Unit]
  
}