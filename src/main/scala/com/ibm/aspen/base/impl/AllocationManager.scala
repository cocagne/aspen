package com.ibm.aspen.base.impl

import com.ibm.aspen.core.allocation.AllocationRecoveryState
import scala.concurrent.Future
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxFinalized
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID

object AllocationManager {
  case class TrackingStatus(saved: Future[Unit], resolved: Future[Boolean]) 
}

trait AllocationManager {
  import AllocationManager._
  
  def trackAllocation(ars: AllocationRecoveryState): TrackingStatus
  
  def receive(resolved: TxResolved): Unit
  def receive(finalized: TxFinalized): Unit
  
  def stopTracking(storeId: DataStoreID, transactionUUID: UUID, committed: Boolean): Unit
}

