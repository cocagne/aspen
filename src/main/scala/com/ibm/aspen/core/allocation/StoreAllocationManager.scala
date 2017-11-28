package com.ibm.aspen.core.allocation

import scala.concurrent.Future
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxFinalized
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.DataStore

trait StoreAllocationManager {
  /** Future returns after the state is saved in the CrashRecoveryLog */
  def trackAllocation(store: DataStore, ars: AllocationRecoveryState): Future[AllocationRecoveryState]
  
  def receive(resolved: TxResolved): Unit
  def receive(finalized: TxFinalized): Unit
}

