package com.ibm.aspen.base.impl

import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.read.ReadType
import java.util.UUID

import com.ibm.aspen.base.AspenSystem

import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.read.BaseReadDriver

import scala.concurrent.duration._
import com.ibm.aspen.core.read.ReadDriver

object SuperSimpleRetryingReadDriver {
  def factory(opportunisticRebuildDelay: Duration, ec: ExecutionContext)(
      transactionCache: TransactionStatusCache,
      clientMessenger: ClientSideReadMessenger,
      objectPointer: ObjectPointer,
      readType: ReadType,
      retrieveLockedTransaction: Boolean,
      readUUID:UUID,
      disableOpportunisticRebuild: Boolean): ReadDriver = {
    new SuperSimpleRetryingReadDriver(transactionCache, clientMessenger, objectPointer, readType,
      retrieveLockedTransaction, readUUID, opportunisticRebuildDelay, disableOpportunisticRebuild)(ec)
  }
}

class SuperSimpleRetryingReadDriver(
    transactionCache: TransactionStatusCache,
    clientMessenger: ClientSideReadMessenger,
    objectPointer: ObjectPointer,
    readType: ReadType,
    retrieveLockedTransaction: Boolean, 
    readUUID:UUID,
    opportunisticRebuildDelay: Duration,
    disableOpportunisticRebuild: Boolean)(implicit ec: ExecutionContext) extends BaseReadDriver(
        transactionCache, clientMessenger, objectPointer,
        readType, retrieveLockedTransaction, readUUID, opportunisticRebuildDelay, disableOpportunisticRebuild)  {

  private var retries = 0
  
  val retryTask: BackgroundTask.ScheduledTask = BackgroundTask.schedulePeriodic(period=Duration(1000, MILLISECONDS)) {
    synchronized {
      retries += 1
      if (retries % 3 == 0) {
        logger.info(s"***** HUNG READ of object ${objectPointer.uuid}. Read UUID $readUUID")
        objectReader.debugLogStatus(s => logger.info(s"* $s"))
      }
    }
    sendReadRequests()
  }

  //println(s"Beginning read of object ${objectPointer.uuid}")
  readResult.onComplete {
    _ =>
      synchronized {
        retryTask.cancel()
        if (retries > 3)
          logger.info(s"***** HUNG READ COMPLETED for object ${objectPointer.uuid}. Read UUID $readUUID")
      }
  }
}