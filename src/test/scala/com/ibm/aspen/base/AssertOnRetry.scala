package com.ibm.aspen.base

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext

class AssertOnRetry(implicit ec: ExecutionContext) extends RetryStrategy {
  def retryUntilSuccessful[T](attempt: => Future[T]): Future[T] = {
    val p = Promise[T]()
    attempt onComplete {
      case Success(r) => p.success(r)
      case Failure(cause) => 
        println("********************* RetryUntilSuccess Operation FAILED *********************")
        assert(false)
        //p.failure(cause)
    }
    p.future
  }
  
  def shutdown(): Unit = ()
}