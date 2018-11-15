package com.ibm.aspen.core

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

class TestActionContext {
  private val executorService = java.util.concurrent.Executors.newScheduledThreadPool( 1)

  implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

  private val shutdownPromise = Promise[Unit]()

  def schedule(delay: Duration)(fn: => Unit): Unit = synchronized {
    executorService.schedule(new Runnable { override def run(): Unit = fn }, delay.length, delay.unit)
  }

  def act(fn: => Unit): Unit = synchronized {
    if (!shutdownPromise.isCompleted)
      executionContext.execute(() => fn)
  }

  def shutdown(): Future[Unit] = {
    act {
      synchronized {
        if (!shutdownPromise.isCompleted) {
          executorService.shutdown()
          shutdownPromise.success(())
        }
      }
    }
    shutdownPromise.future
  }
}
