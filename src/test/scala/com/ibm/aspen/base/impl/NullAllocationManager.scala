package com.ibm.aspen.base.impl

import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.data_store.DataStoreID
import java.util.UUID
import scala.concurrent.Future

class NullAllocationManager extends AllocationManager {
  import AllocationManager._
  
  def trackAllocation(ars: AllocationRecoveryState): TrackingStatus = TrackingStatus(Future.successful(()), Future.successful(true))
  
  def receive(resolved: TxResolved): Unit = ()
  def receive(finalized: TxFinalized): Unit = ()
  
  def stopTracking(storeId: DataStoreID, transactionUUID: UUID, committed: Boolean): Unit = ()
}