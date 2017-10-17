package com.ibm.aspen.base

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext

class NoRetry(implicit ec: ExecutionContext) extends RetryStrategy {
  def retryUntilSuccessful[T](attempt: => Future[T]): Future[T] = {
    val p = Promise[T]()
    attempt onComplete {
      case Success(r) => p.success(r)
      case Failure(cause) => p.failure(cause)
    }
    p.future
  }
}