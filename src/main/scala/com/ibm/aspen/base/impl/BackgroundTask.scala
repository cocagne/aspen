package com.ibm.aspen.base.impl

import java.util.concurrent.{Executors, ScheduledFuture, ThreadLocalRandom}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, MILLISECONDS}

object BackgroundTask {

  trait ScheduledTask {
    def cancel(): Unit
  }

  private [this] var sched = Executors.newScheduledThreadPool(1)
  private [this] var ec = ExecutionContext.fromExecutorService(sched)
  private [this] val rand = new java.util.Random
  

  def resizeThreadPool(numThreads: Int): Unit = synchronized {
    sched.shutdown() // Previously submitted tasks will be executed before the pool is destroyed
    sched = Executors.newScheduledThreadPool(numThreads)
    ec = ExecutionContext.fromExecutorService(sched)
  }

  private case class BGTask[T](sf: ScheduledFuture[T]) extends ScheduledTask {
    override def cancel(): Unit = sf.cancel(false)
  }
  
  def schedule(delay: Duration)(fn: => Unit): ScheduledTask = synchronized {
    BGTask(sched.schedule(new Runnable { override def run(): Unit = fn }, delay.length, delay.unit))
  }
  
  def scheduleRandomlyWithinWindow(window: Duration)(fn: => Unit): ScheduledTask = synchronized {
    // TODO: Fix Long -> Int conversion
    val actualDelay = rand.nextInt(window.length.asInstanceOf[Int])
    
    BGTask(sched.schedule(new Runnable { override def run(): Unit = fn }, actualDelay, window.unit))
  }
  
  /** initialDelay uses the same units as the period 
   *
   * @param callNow Defaults to false. If true, the function will be executed immediately otherwise it waits for the polling period to elapse   
   */
  def schedulePeriodic(period: Duration, callNow: Boolean=false)(fn: => Unit): ScheduledTask = synchronized {
    val initialDelay = if (callNow) 0L else period.length
    BGTask(sched.scheduleAtFixedRate(() => fn, initialDelay, period.length, period.unit))
  }
  
  /** Continually retries the function until it returns true */
  case class RetryWithExponentialBackoff(tryNow: Boolean, initialDelay: Duration, maxDelay: Duration)(fn: => Boolean) extends ScheduledTask {
    private[this] var task: Option[ScheduledTask] = None
    private[this] var backoffDelay = initialDelay

    if (tryNow)
      attempt()
    else
      reschedule(false)

    private def attempt(): Unit = synchronized {
      if (!fn) reschedule(true)
    }

    override def cancel(): Unit = synchronized {
      task.foreach(_.cancel())
      task = None
    }
    
    private def reschedule(backoff: Boolean): Unit = synchronized {
      val thisDelay = if (backoff) {
        backoffDelay = backoffDelay * 2
        if (backoffDelay > maxDelay)
          backoffDelay = maxDelay

        Duration(ThreadLocalRandom.current().nextInt(0, backoffDelay.toMillis.asInstanceOf[Int]), MILLISECONDS)
      }
      else
        backoffDelay

      task = Some(schedule(thisDelay) { attempt() })
    }
  }
}