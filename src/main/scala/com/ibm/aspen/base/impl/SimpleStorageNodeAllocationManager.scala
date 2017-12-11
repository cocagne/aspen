package com.ibm.aspen.base.impl

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.network.StoreSideAllocationMessenger
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ Duration, MILLISECONDS }
import java.util.UUID


class SimpleStorageNodeAllocationManagerclass(
    val heartbeatPeriod: Duration,
    val heartbeatTimeout: Duration,
    crl: CrashRecoveryLog, 
    allocationMessenger: StoreSideAllocationMessenger)
  (implicit ec: ExecutionContext) extends StorageNodeAllocationManager(crl, allocationMessenger) {
  
  import StorageNodeAllocationManager._
  
  private[this] var recovering = Set[Key]()
  
  // Periodically check out heartbeats for all outstanding allocation transactions. If any haven't received
  // heart beat updates within the timeout window, begin the recovery process
  //
  BackgroundTask.schedulePeriodic(heartbeatPeriod) {
    val now = System.currentTimeMillis()
    
    val (allocs, recovers) = synchronized { (allocations, recovering) }
    
    allocs.foreach { t =>
      val (key, value) = t
      if (Duration(now - value.lastHeartbeatTimestamp, MILLISECONDS) > heartbeatTimeout && ! recovers.contains(key))
        recover(key, value)
    }
  }
  
  def recover(key: Key, value: Value) = {
    synchronized { recovering += key }
    
    // TODO, Rest of the recovery process. Split out into a separate Object? Will need access to AspenSystem & alloc messenger
  }
}