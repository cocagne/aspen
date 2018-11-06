package com.ibm.aspen.base.impl

import scala.concurrent.duration._
import com.ibm.aspen.core.allocation.{AllocationDriver, AllocationOptions, AllocationRevisionGuard, BaseAllocationDriver}
import com.ibm.aspen.core.network.ClientSideAllocationMessenger
import java.util.UUID

import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision

import scala.concurrent.ExecutionContext

object SuperSimpleRetryingAllocationDriver {
  
  def factory(retransmitDelay: Duration)(implicit ec: ExecutionContext): AllocationDriver.Factory = {
    return new AllocationDriver.Factory {
      def create(messenger: ClientSideAllocationMessenger,
                 poolUUID: UUID,
                 newObjectUUID: UUID,
                 objectSize: Option[Int],
                 objectIDA: IDA,
                 objectData: Map[Byte,DataBuffer], // Map DataStore pool index -> store-specific ObjectData
                 options: AllocationOptions,
                 timestamp: HLCTimestamp,
                 initialRefcount: ObjectRefcount,
                 allocationTransactionUUID: UUID,
                 revisionGuard: AllocationRevisionGuard): BaseAllocationDriver = {
        new SuperSimpleRetryingAllocationDriver(retransmitDelay, messenger, poolUUID, newObjectUUID, objectSize, objectIDA, objectData, options,  
                             timestamp, initialRefcount, allocationTransactionUUID, revisionGuard)
      }
    }
  }

}

class SuperSimpleRetryingAllocationDriver(
    retransmitDelay: Duration,
    messenger: ClientSideAllocationMessenger,
    poolUUID: UUID,
    newObjectUUID: UUID,
    objectSize: Option[Int],
    objectIDA: IDA,
    objectData: Map[Byte,DataBuffer], // Map DataStore pool index -> store-specific ObjectData
    options: AllocationOptions,
    timestamp: HLCTimestamp,
    initialRefcount: ObjectRefcount,
    allocationTransactionUUID: UUID,
    revisionGuard: AllocationRevisionGuard) (implicit ec: ExecutionContext) extends BaseAllocationDriver(
        messenger, poolUUID, newObjectUUID, objectSize, objectIDA,
        objectData, options, timestamp, initialRefcount, allocationTransactionUUID, revisionGuard) {
  
  val retryTask = BackgroundTask.schedulePeriodic(period=retransmitDelay, callNow=false)( sendAllocationMessages() )
  
  futureResult.onComplete { case _ => retryTask.cancel() }
}