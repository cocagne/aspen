package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.network.StoreSideAllocationMessenger
import com.ibm.aspen.core.data_store.DataStore
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.data_store.DataStoreID

class StoreAllocationManager(
    val storeMessenger: StoreSideAllocationMessenger,
    val driverFactory: AllocationDriver.Factory,
    initialStores: List[DataStore])(implicit ec: ExecutionContext) {
  
  private[this] var dataStores: Map[DataStoreID,DataStore] = initialStores.map(ds => (ds.storeId -> ds)).toMap
  
  def addStore(ds: DataStore) = synchronized { dataStores += (ds.storeId -> ds) }
  
  def removeStore(ds: DataStore) = synchronized { dataStores -= ds.storeId }
  
  def receiveAllocateMessage(m: Allocate): Unit = {
    synchronized { dataStores.get(m.toStore) } foreach {
      ds => 
        val f = ds.allocateNewObject(m.newObjectUUID, m.objectSize, m.objectData, m.initialRefcount, m.allocationTransactionUUID, 
                                     m.allocatingObject, m.allocatingObjectRevision)
                                     
        f onSuccess {
          case result => storeMessenger.send(m.fromClient, AllocateResponse(m.toStore, m.allocationTransactionUUID, result))
        }
    }
  }
}