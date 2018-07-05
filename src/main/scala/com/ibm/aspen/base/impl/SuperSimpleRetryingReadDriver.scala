package com.ibm.aspen.base.impl

import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.read.ReadType
import java.util.UUID
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.read.BaseReadDriver
import scala.concurrent.duration._
import com.ibm.aspen.core.read.ReadDriver

object SuperSimpleRetryingReadDriver {
  def factory(opportunisticRebuildDelay: Duration, ec: ExecutionContext)(
      clientMessenger: ClientSideReadMessenger,
      objectPointer: ObjectPointer,
      readType: ReadType,
      retrieveLockedTransaction: Boolean,
      readUUID:UUID,
      disableOpportunisticRebuild: Boolean): ReadDriver = {
    new SuperSimpleRetryingReadDriver(clientMessenger, objectPointer, readType, retrieveLockedTransaction, readUUID, opportunisticRebuildDelay, disableOpportunisticRebuild)(ec)
  }
}

class SuperSimpleRetryingReadDriver(
    clientMessenger: ClientSideReadMessenger,
    objectPointer: ObjectPointer,
    readType: ReadType,
    retrieveLockedTransaction: Boolean, 
    readUUID:UUID,
    opportunisticRebuildDelay: Duration,
    disableOpportunisticRebuild: Boolean)(implicit ec: ExecutionContext) extends BaseReadDriver(clientMessenger, objectPointer, 
        readType, retrieveLockedTransaction, readUUID, opportunisticRebuildDelay, disableOpportunisticRebuild)  {
  
  val retryTask = BackgroundTask.schedulePeriodic(period=Duration(250, MILLISECONDS), callNow=false)( sendReadRequests() )
  //println(s"Beginning read of object ${objectPointer.uuid}")
  readResult.onComplete {
    case _ => 
      retryTask.cancel()
      //println(s"    Read complete for object ${objectPointer.uuid}")
  }
}