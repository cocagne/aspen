package com.ibm.aspen.cumulofs

import java.io.IOException

import com.ibm.aspen.base.{AspenSystem, StopRetrying, Transaction}
import com.ibm.aspen.core.read.FatalReadError

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

package object nfs {

  def blockingCall[T](fn: => Future[T])(implicit ec: ExecutionContext): T = try {
    Await.result(fn, Duration.Inf)
  } catch {
    case _: FatalReadError => throw new IOException("Invalid Aspen Object")
    case t: Throwable => throw t
  }

  def retryTransactionUntilSuccessful[T](system: AspenSystem,
                                         refreshOnFailure: List[NFSBaseFile]=Nil)(prepare: Transaction => Future[T])(implicit ec: ExecutionContext): T = blockingCall {
    def onFail(err: Throwable): Future[Unit] = err match {
      case err: FatalReadError => throw StopRetrying(err)
      case _ => Future.sequence(refreshOnFailure.map(_.file.refresh())).map(_=>())
    }
    system.transactUntilSuccessfulWithRecovery(onFail) { tx =>
      prepare(tx)
    }
  }

}
