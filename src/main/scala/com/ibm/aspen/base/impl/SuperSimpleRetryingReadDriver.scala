package com.ibm.aspen.base.impl

import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.read.ReadType
import java.util.UUID

import com.ibm.aspen.base.{AspenSystem, ObjectCache, OpportunisticRebuildManager}

import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.read.BaseReadDriver

import scala.concurrent.duration._
import com.ibm.aspen.core.read.ReadDriver

object SuperSimpleRetryingReadDriver {
  def factory(ec: ExecutionContext)(
      objectCache: ObjectCache,
      opRebuildManager: OpportunisticRebuildManager,
      transactionCache: TransactionStatusCache,
      clientMessenger: ClientSideReadMessenger,
      objectPointer: ObjectPointer,
      readType: ReadType,
      retrieveLockedTransaction: Boolean,
      readUUID:UUID,
      disableOpportunisticRebuild: Boolean): ReadDriver = {
    new SuperSimpleRetryingReadDriver(objectCache, opRebuildManager, transactionCache, clientMessenger, objectPointer,
      readType, retrieveLockedTransaction, readUUID, disableOpportunisticRebuild)(ec)
  }
}

class SuperSimpleRetryingReadDriver(
    objectCache: ObjectCache,
    opRebuildManager: OpportunisticRebuildManager,
    transactionCache: TransactionStatusCache,
    clientMessenger: ClientSideReadMessenger,
    objectPointer: ObjectPointer,
    readType: ReadType,
    retrieveLockedTransaction: Boolean, 
    readUUID:UUID,
    disableOpportunisticRebuild: Boolean)(implicit ec: ExecutionContext) extends BaseReadDriver(
        objectCache, opRebuildManager, transactionCache, clientMessenger, objectPointer,
        readType, retrieveLockedTransaction, readUUID, disableOpportunisticRebuild)  {

  private var retries = 0

  private var oretryTask: Option[BackgroundTask.ScheduledTask] = None

  override def begin(): Unit = synchronized {

    val rt = BackgroundTask.schedulePeriodic(period=Duration(1000, MILLISECONDS)) {
      synchronized {
        retries += 1

        val now = System.currentTimeMillis()

        objectReader.rereadCandidates.foreach { t =>
          if (now - t._2.wallTime > 1000) {
            logger.info(s"ReadUUID $readUUID - Sending reread request to known-behind store ${t._1}")
            sendReadRequest(t._1)
          }
        }

        if (retries % 3 == 0)
          objectReader.debugLogStatus(readUUID, s"***** HUNG READ of object ${objectPointer.uuid}. Read UUID $readUUID", s => logger.info(s"* $s"))

        if (retries % 10 == 0) {
          val noResponses = objectReader.noResponses
          if (noResponses.nonEmpty) {
            logger.info(s"ReadUUID $readUUID - Resending reads to non responding stores $noResponses")
            noResponses.foreach(sendReadRequest)
          }
        }
      }
    }

    oretryTask = Some(rt)

    super.begin()
  }


  //println(s"Beginning read of object ${objectPointer.uuid}")
  readResult.onComplete {
    _ =>
      synchronized {
        oretryTask.foreach(_.cancel())
        if (retries > 3)
          logger.info(s"***** HUNG READ COMPLETED for object ${objectPointer.uuid}. Read UUID $readUUID")
      }
  }
}