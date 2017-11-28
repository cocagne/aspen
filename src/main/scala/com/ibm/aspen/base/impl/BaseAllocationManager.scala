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
import com.ibm.aspen.core.data_store.DataStore
import scala.concurrent.ExecutionContext

object BaseAllocationManager {
  case class Key(storeId: DataStoreID, transactionUUID: UUID)
  
  case class Value(saved: Future[AllocationRecoveryState], store:DataStore, ars: AllocationRecoveryState) 
}

class BaseAllocationManager(crl: CrashRecoveryLog)(implicit ec: ExecutionContext) extends StoreAllocationManager {
  
  import BaseAllocationManager._
  
  protected var allocations = Map[Key, Value]()
  
  def trackAllocation(store: DataStore, ars: AllocationRecoveryState): Future[AllocationRecoveryState] = synchronized {
    val key = Key(ars.storeId, ars.allocationTransactionUUID)
    val value = allocations.get(key) match {
      case Some(v) => v
      case None =>
        val fsaved = crl.saveAllocationRecoveryState(ars).map(_=>ars)
        val v = Value(fsaved, store, ars)
        allocations += (key -> v)
        v
    }
    value.saved
  }
  
  def receive(resolved: TxResolved): Unit = stopTracking(resolved.to, resolved.transactionUUID, resolved.committed) 
  def receive(finalized: TxFinalized): Unit = stopTracking(finalized.to, finalized.transactionUUID, finalized.committed)
  
  private def stopTracking(storeId: DataStoreID, transactionUUID: UUID, committed: Boolean): Unit = synchronized {
    val key = Key(storeId, transactionUUID)
    
    allocations.get(key).foreach{ v =>
      allocations -= key
      
      v.store.allocationResolved(v.ars, committed).foreach { _ =>
        crl.discardAllocationState(storeId, transactionUUID)
      }
    }
  }
}