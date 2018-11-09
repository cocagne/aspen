package com.ibm.aspen.base.impl

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.network.StoreSideAllocationMessenger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, MILLISECONDS}


class SimpleStorageNodeAllocationManager(
    val heartbeatPeriod: Duration,
    val allocationTimeout: Duration,
    val statusQueryPeriod: Duration,
    val system: BasicAspenSystem,
    crl: CrashRecoveryLog, 
    allocationMessenger: StoreSideAllocationMessenger)
  (implicit ec: ExecutionContext) extends StorageNodeAllocationManager(crl, allocationMessenger) {
  
  import StorageNodeAllocationManager._
  
  private[this] var recoveryProcesses = Map[Key, SimpleAllocationRecoveryProcess]()
  
  // Periodically check out heartbeats for all outstanding allocation transactions. If any haven't received
  // heart beat updates within the timeout window, begin the recovery process
  //
  private[this] val bgTask = BackgroundTask.schedulePeriodic(heartbeatPeriod) { synchronized {
    val now = System.currentTimeMillis()
    
    val (allocs, recovers) = synchronized { (allocations, recoveryProcesses) }
    
    allocs.foreach { t =>
      val (key, m) = t
      m.values.foreach { value =>
        if (Duration(now - value.lastHeartbeatTimestamp, MILLISECONDS) > allocationTimeout && ! recovers.contains(key)) {
          val rp = new SimpleAllocationRecoveryProcess()
          
          recoveryProcesses += (key -> rp)
          
          rp.done foreach { committed => synchronized {
            recoveryProcesses -= key
            stopTracking(key.storeId, key.transactionUUID, committed)
          }}
        }
      }
    }
  }}
  /*
  override def receive(message: AllocationStatusReply): Unit = synchronized {
    recoveryProcesses.get(Key(message.to, message.allocationTransactionUUID)).foreach { rp =>
      rp.receive(message)
    }
  }
  */
  override def shutdown(): Unit = bgTask.cancel()
}