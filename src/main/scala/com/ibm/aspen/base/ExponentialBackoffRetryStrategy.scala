package com.ibm.aspen.base

import scala.concurrent.Future
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import java.util.concurrent.TimeUnit

object ExponentialBackoffRetryStrategy {
  private [this] var scheduler = Executors.newScheduledThreadPool(1)
  private [this] var ec = ExecutionContext.fromExecutorService(scheduler)
  
  def resizeThreadPool(numThreads: Int): Unit = synchronized {
    scheduler.shutdown() // Previously submitted tasks will be executed before the pool is destroyed
    scheduler = Executors.newScheduledThreadPool(numThreads)
    ec = ExecutionContext.fromExecutorService(scheduler)
  }
  
  def getScheduler(): ScheduledExecutorService = synchronized { scheduler }
  
  def getExecutionContext(): ExecutionContext = synchronized { ec }
  
  val rand = new java.util.Random
}

class ExponentialBackoffRetryStrategy(backoffLimit: Int = 60 * 1000, initialRetryDelay: Int = 15) extends RetryStrategy {
  import ExponentialBackoffRetryStrategy._
  
  def retryUntilSuccessful[T](attempt: => Future[T]): Future[T] = {
    val p = Promise[T]()
    
    implicit val ec = getExecutionContext()
    
    def retry(limit: Int): Unit = attempt onComplete {
      case Success(result) => p.success(result)
      
      case Failure(cause) => cause match {
        case StopRetrying(reason) => p.failure(reason)
        
        case _ =>
          val delay = rand.nextInt(limit)
          val nextLimit = if (limit * limit < backoffLimit) limit * limit else backoffLimit
          getScheduler().schedule(new Runnable { override def run(): Unit = retry(nextLimit) }, delay, TimeUnit.MILLISECONDS)
      }
    }
    
    retry(initialRetryDelay)
    
    p.future
  }
}