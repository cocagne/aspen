package com.ibm.aspen.base

import java.util.UUID
import scala.concurrent.Future
import com.ibm.aspen.core.objects.keyvalue.Key

object Task {
  val TaskTypeKey = Key(Array[Byte](0)) // Corresponds to TaskType UUID
  
  val IdleTaskType = new UUID(0,0)
}

trait Task {
  
  val taskPointer: TaskPointer
  
  def completed: Future[Unit]
  
  def resume(): Unit
  
  def suspend(): Unit
}