package com.ibm.aspen.base

import java.util.UUID

import scala.concurrent.Future

trait FinalizationAction {

  /** Transaction UUID that spawned this FinalizationAction
    */
  val parentTransactionUUID: UUID

  /** Future completes when the action is completed or completionDetected() is called.
   *
   * Note, all retry logic must be included within the execute() method. The future
   * MUST NOT return until the action is completed, is detected to have already been
   * completed by the execute logic, or completionDetected() is called (which will
   * occur if a peer completes the action and broadcasts the event to peers/clients)
   */
  val complete: Future[Unit]
  
  /** Called when we are explicitly informed that some other peer has completed the action */
  def completionDetected(): Unit = ()
}