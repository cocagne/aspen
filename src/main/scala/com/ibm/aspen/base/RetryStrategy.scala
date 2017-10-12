package com.ibm.aspen.base

import scala.concurrent.Future

trait RetryStrategy {
  def retryUntilSuccessful[T](attempt: => Future[T]): Future[T]
}