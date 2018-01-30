package com.ibm.aspen.base.impl

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.network.StoreSideAllocationMessenger
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ Duration, MILLISECONDS }
import java.util.UUID


class SimpleStorageNodeAllocationManagerclass(
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
      val (key, value) = t
      if (Duration(now - value.lastHeartbeatTimestamp, MILLISECONDS) > allocationTimeout && ! recovers.contains(key)) {
        val rp = new SimpleAllocationRecoveryProcess(statusQueryPeriod, system, allocationMessenger, crl, value.store, value.ars)
        
        recoveryProcesses += (key -> rp)
        
        rp.done foreach { _ => synchronized {
          recoveryProcesses -= key
        }}
      }
    }
  }}
  
  override def shutdown(): Unit = bgTask.cancel()
}