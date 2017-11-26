package com.ibm.aspen.base.impl

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.data_store.DataStoreID
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.Promise
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.allocation.StoreAllocationManager

object BaseAllocationManager {
  case class Key(storeId: DataStoreID, transactionUUID: UUID)
  
  case class Value(saved: Future[Unit], ars: AllocationRecoveryState) {
    val resolved = Promise[Boolean]()
  }
}

class BaseAllocationManager(crl: CrashRecoveryLog) extends StoreAllocationManager {
  import StoreAllocationManager._
  import BaseAllocationManager._
  
  protected var allocations = Map[Key, Value]()
  
  def trackAllocation(ars: AllocationRecoveryState): TrackingStatus = synchronized {
    val key = Key(ars.storeId, ars.allocationTransactionUUID)
    val value = allocations.get(key) match {
      case Some(v) => v
      case None =>
        val fsaved = crl.saveAllocationRecoveryState(ars)
        val v = Value(fsaved, ars)
        allocations += (key -> v)
        v
    }
    TrackingStatus(value.saved, value.resolved.future)
  }
  
  def receive(resolved: TxResolved): Unit = stopTracking(resolved.to, resolved.transactionUUID, resolved.committed) 
  def receive(finalized: TxFinalized): Unit = stopTracking(finalized.to, finalized.transactionUUID, finalized.committed)
  
  def stopTracking(storeId: DataStoreID, transactionUUID: UUID, committed: Boolean): Unit = synchronized {
    val key = Key(storeId, transactionUUID)
    allocations.get(key).foreach{ v =>
      if (!v.resolved.isCompleted) {
        allocations -= key
        crl.discardAllocationState(storeId, transactionUUID)
        v.resolved.success(committed)
      }
    }
  }
}