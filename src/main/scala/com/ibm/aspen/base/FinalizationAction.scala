package com.ibm.aspen.base

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait FinalizationAction {
  
  /** Future completes when the action is completed or completionDetected() is called.
   *
   * Note, all retry logic must be included within the execute() method. The future
   * MUST NOT return until the action is completed, is detected to have already been
   * completed by the execute logic, or completionDetected() is called (which will
   * occur if a peer completes the action and broadcasts the event to peers/clients)
   */
  def execute()(implicit ec: ExecutionContext): Future[Unit]
  
  /** Called when we are explicitly informed that some other peer has completed the action */
  def completionDetected(): Unit = ()
}