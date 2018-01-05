package com.ibm.aspen.base

import java.util.UUID
import scala.concurrent.Future

trait Task {
  val taskType: TaskType

  val taskUUID: UUID
  
  def complete: Future[Unit]
  
  def resume(): Unit
  
  def pause(): Unit
}