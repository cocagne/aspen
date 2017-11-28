package com.ibm.aspen.base.impl

import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.data_store.DataStoreID
import java.util.UUID
import scala.concurrent.Future
import com.ibm.aspen.core.allocation.StoreAllocationManager
import com.ibm.aspen.core.data_store.DataStore

class NullAllocationManager extends StoreAllocationManager {
  
  def trackAllocation(store: DataStore, ars: AllocationRecoveryState): Future[AllocationRecoveryState] = Future.successful(ars)
  
  def receive(resolved: TxResolved): Unit = ()
  def receive(finalized: TxFinalized): Unit = ()
}