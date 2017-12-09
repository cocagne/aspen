package com.ibm.aspen.base.impl

import scala.concurrent.Future
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import java.util.concurrent.TimeUnit
import java.util.concurrent.ScheduledFuture
import scala.concurrent.duration.Duration

object BackgroundTask {

  private [this] var sched = Executors.newScheduledThreadPool(1)
  private [this] var ec = ExecutionContext.fromExecutorService(scheduler)
  private [this] val rand = new java.util.Random
  
  private [this] def scheduler = synchronized { sched }
  private [this] def executionContext = synchronized { ec }
  
  def resizeThreadPool(numThreads: Int): Unit = synchronized {
    sched.shutdown() // Previously submitted tasks will be executed before the pool is destroyed
    sched = Executors.newScheduledThreadPool(numThreads)
    ec = ExecutionContext.fromExecutorService(scheduler)
  }
  
  trait ScheduledTask {
    def cancel(): Unit
  }
  
  private case class BGTask[T](sf: ScheduledFuture[T]) extends ScheduledTask {
    override def cancel(): Unit = sf.cancel(false)
  }
  
  def schedule(delay: Duration)(fn: => Unit): ScheduledTask = {
    BGTask(scheduler.schedule(new Runnable { override def run(): Unit = fn }, delay.length, delay.unit))
  }
  
  def scheduleRandomlyWithinWindow(window: Duration)(fn: => Unit): ScheduledTask = {
    // TODO: Fix Long -> Int conversion
    val actualDelay = rand.nextInt(window.length.asInstanceOf[Int])
    
    BGTask(scheduler.schedule(new Runnable { override def run(): Unit = fn }, actualDelay, window.unit))
  }
  
  /** initialDelay uses the same units as the period */
  def schedulePeriodic(period: Duration, initialDelay: Option[Long] = None)(fn: => Unit): ScheduledTask = {
    BGTask(scheduler.scheduleAtFixedRate(new Runnable { override def run(): Unit = fn }, initialDelay.getOrElse(period.length), period.length, period.unit))
  }
  
  /** Continually retries the function until it returns true */
  case class retryWithExponentialBackoff(tryNow: Boolean, initialDelay: Duration, maxDelay: Duration)(fn: => Boolean) extends ScheduledTask {
    private[this] var task: Option[ScheduledTask] = None
    private[this] var backoffDelay = initialDelay
    
    override def cancel(): Unit = synchronized { task.foreach(_.cancel()) } 
    
    private def reschedule(backoff: Boolean): Unit = synchronized {
      if (backoff) {
        backoffDelay = backoffDelay * 2
        if (backoffDelay > maxDelay)
          backoffDelay = maxDelay
      }
      task = Some(schedule(backoffDelay) { attempt() }) 
    }
    
    if (tryNow)
      attempt()
    else
      reschedule(false)
    
    private def attempt(): Unit = if (!fn) reschedule(true) 
  }
}