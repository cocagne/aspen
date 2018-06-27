package com.ibm.aspen.base

import scala.concurrent.Future

trait MissedUpdateHandler {
  val complete: Future[Unit]
  
  def execute(): Unit
}