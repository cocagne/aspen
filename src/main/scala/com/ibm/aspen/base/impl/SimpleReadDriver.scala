package com.ibm.aspen.base.impl

import com.ibm.aspen.core.read.BaseReadDriver
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.objects.ObjectPointer
import java.util.UUID

import com.ibm.aspen.base.{ObjectCache, OpportunisticRebuildManager}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import com.ibm.aspen.core.read.ReadDriver
import com.ibm.aspen.core.read.ReadType

object SimpleReadDriver {
  class Factory(
      val initialDelay: Duration, 
      val maxDelay: Duration)(implicit ec: ExecutionContext) {
    def apply(
        objectCache: ObjectCache,
        opRebuildManager: OpportunisticRebuildManager,
        transactionCache: TransactionStatusCache,
        clientMessenger: ClientSideReadMessenger,
        objectPointer: ObjectPointer,
        readType: ReadType,
        retrieveLockedTransaction: Boolean, 
        readUUID:UUID): ReadDriver = {
      new SimpleReadDriver(initialDelay, maxDelay, objectCache, opRebuildManager, transactionCache, clientMessenger,
        objectPointer, readType, retrieveLockedTransaction, readUUID)
    }
  }
}

/** This class provides a *very* simple exponential backoff retry mechanism for reads in that completes when either the object
 *  is successfully read or a fatal error is encountered.
 * 
 */
class SimpleReadDriver(
    val initialDelay: Duration, 
    val maxDelay: Duration,
    objectCache: ObjectCache,
    opRebuildManager: OpportunisticRebuildManager,
    transactionCache: TransactionStatusCache,
    clientMessenger: ClientSideReadMessenger,
    objectPointer: ObjectPointer,
    readType: ReadType,
    retrieveLockedTransaction: Boolean, 
    readUUID:UUID)(implicit ec: ExecutionContext) extends BaseReadDriver(
        objectCache, opRebuildManager, transactionCache, clientMessenger, objectPointer, readType, retrieveLockedTransaction, readUUID) {
  
  private[this] var task: Option[BackgroundTask.ScheduledTask] = None

  readResult.onComplete { _ => synchronized {
    task.foreach(_.cancel())
  }}
  
  override def begin(): Unit = synchronized {
    task = Some(BackgroundTask.RetryWithExponentialBackoff(tryNow=true, initialDelay=initialDelay, maxDelay=maxDelay) {
      super.begin()
      false
    })
  }
  
  override def shutdown(): Unit = task.foreach( t => t.cancel() )
}