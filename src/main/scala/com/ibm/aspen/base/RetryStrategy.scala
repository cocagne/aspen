package com.ibm.aspen.base

import scala.concurrent.Future

trait RetryStrategy {
  def retryUntilSuccessful(attempt: => Future[Unit]): Future[Unit]
}