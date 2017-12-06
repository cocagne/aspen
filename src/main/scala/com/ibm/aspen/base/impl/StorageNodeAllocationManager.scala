package com.ibm.aspen.base.impl

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.data_store.DataStoreID
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.Promise
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.data_store.DataStore
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.allocation.AllocationErrors
import com.ibm.aspen.core.allocation.AllocateResponse
import com.ibm.aspen.core.network.StoreSideAllocationMessenger
import com.ibm.aspen.core.network.StoreSideAllocationMessageReceiver

object StorageNodeAllocationManager {
  case class Key(storeId: DataStoreID, transactionUUID: UUID)
  
  case class Value(saved: Future[AllocationRecoveryState], store:DataStore, ars: AllocationRecoveryState) 
}

class StorageNodeAllocationManager(
    val crl: CrashRecoveryLog, 
    val allocationMessenger: StoreSideAllocationMessenger)
  (implicit ec: ExecutionContext) extends StoreSideAllocationMessageReceiver {
  
  import StorageNodeAllocationManager._
  
  protected[this] var allocations = Map[Key, Value]()
  protected[this] var stores = Map[DataStoreID, DataStore]()
    
  protected[this] def getStore(sid: DataStoreID) = synchronized { stores.get(sid) }
  
  def addStore(store: DataStore): Unit = { 
    val lars = crl.getAllocationRecoveryStateForStore(store.storeId)
    lars.foreach(ars => trackAllocation(store, ars))
    synchronized { 
      stores += (store.storeId -> store) 
    }
  } 
  
  private def trackAllocation(store: DataStore, ars: AllocationRecoveryState): Future[AllocationRecoveryState] = synchronized {
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
  
  private def stopTracking(storeId: DataStoreID, transactionUUID: UUID, committed: Boolean): Unit = synchronized {
    val key = Key(storeId, transactionUUID)
    
    allocations.get(key).foreach{ v =>
      allocations -= key
      
      v.store.allocationResolved(v.ars, committed).foreach { _ =>
        crl.discardAllocationState(storeId, transactionUUID)
      }
    }
  }
  
  def receive(resolved: TxResolved): Unit = stopTracking(resolved.to, resolved.transactionUUID, resolved.committed) 
  def receive(finalized: TxFinalized): Unit = stopTracking(finalized.to, finalized.transactionUUID, finalized.committed)
  
  def receive(m: Allocate): Unit = getStore(m.toStore).foreach{ store => {
      
      def reply(result: Either[AllocationErrors.Value, List[AllocateResponse.Allocated]]) = {
        allocationMessenger.send(m.fromClient, AllocateResponse(m.toStore, m.allocationTransactionUUID, result))
      }
      
      store.allocate(
          m.newObjects, 
          m.allocationTransactionUUID, 
          m.allocatingObject, 
          m.allocatingObjectRevision).foreach { r => r match {
            
          case Left(err) => reply(Left(err))
          
          case Right(ars) => 
            trackAllocation(store, ars) foreach { _ =>
              val allocs = ars.newObjects.map(n => AllocateResponse.Allocated(n.newObjectUUID, n.storePointer)) 
              reply(Right(allocs))
            }
        }
      }
    }}
}