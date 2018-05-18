package com.ibm.aspen.base

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext
import java.io.StringWriter
import java.io.PrintWriter

class AssertOnRetry(implicit ec: ExecutionContext) extends RetryStrategy {
  def retryUntilSuccessful[T](attempt: => Future[T]): Future[T] = {
    val p = Promise[T]()
    attempt onComplete {
      case Success(r) => p.success(r)
      
      case Failure(StopRetrying(cause)) => p.failure(cause)
      
      case Failure(cause) => 
        println(s"********************* RetryUntilSuccess Operation FAILED: $cause")
        assert(false)
        //p.failure(cause)
    }
    p.future
  }
  
  def retryUntilSuccessful[T](onAttemptFailure: (Throwable) => Future[Unit])(attempt: => Future[T]): Future[T] = retryUntilSuccessful {
    val p = Promise[T]()
    attempt onComplete {
      case Success(r) => p.success(r)
      
      case Failure(StopRetrying(cause)) => p.failure(StopRetrying(cause))
      
      case Failure(cause) => 
        // Allow a single attempt to resolve the error
        val sw = new StringWriter();
        val pw = new PrintWriter(sw);
        val stacktrace = cause.printStackTrace(pw);
        //println(s"** INITIAL Attempt failed due to $cause:\n $stacktrace")
        onAttemptFailure(cause) onComplete {
          case Success(r) =>
            attempt onComplete {
              case Success(r) => p.success(r)
              
              case Failure(cause) => 
                println(s"********************* RetryUntilSuccessWithRecovery Operation FAILED *after* recovery operation: $cause")
                assert(false)
                //p.failure(cause)
            }
          
          case Failure(cause) =>
            println(s"********************* RetryUntilSuccessWithRecovery Operation FAILED: $cause")
            assert(false)
            //p.failure(cause)
        }
    }
    p.future
  }
  
  def shutdown(): Unit = ()
}