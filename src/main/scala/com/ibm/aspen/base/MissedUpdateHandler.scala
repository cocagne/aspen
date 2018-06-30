package com.ibm.aspen.base

import scala.concurrent.Future

trait MissedUpdateHandler {
  def execute(): Future[Unit]
}