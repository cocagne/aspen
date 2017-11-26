package com.ibm.aspen.core.allocation

import scala.concurrent.Future
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxFinalized
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID

object StoreAllocationManager {
  case class TrackingStatus(saved: Future[Unit], resolved: Future[Boolean]) 
}

trait StoreAllocationManager {
  import StoreAllocationManager._
  
  def trackAllocation(ars: AllocationRecoveryState): TrackingStatus
  
  def receive(resolved: TxResolved): Unit
  def receive(finalized: TxFinalized): Unit
  
  def stopTracking(storeId: DataStoreID, transactionUUID: UUID, committed: Boolean): Unit
}

