package com.ibm.aspen.base.impl

import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.network.StoreSideReadMessageReceiver
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.DataStore
import com.ibm.aspen.core.network.StoreSideReadMessenger
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import scala.concurrent.Future
import com.ibm.aspen.core.read.Read
import com.ibm.aspen.core.data_store.InvalidLocalPointer
import com.ibm.aspen.core.data_store.ObjectMismatch
import com.ibm.aspen.core.data_store.CorruptedObject
import com.ibm.aspen.core.read.ReadError
import com.ibm.aspen.core.read.ReadResponse

class StorageNodeReadManager(messenger: StoreSideReadMessenger)(implicit ec: ExecutionContext) extends StoreSideReadMessageReceiver {
  
  private[this] var stores = Map[DataStoreID, DataStore]()
  
  private def getStore(sid: DataStoreID) = synchronized { stores.get(sid) }
  
  def hostedStores: List[DataStore] = synchronized { stores.values.toList }
  
  def addStore(store: DataStore): Unit = synchronized { stores += (store.storeId -> store) }
      
  def receive(message: Read): Unit = getStore(message.toStore).foreach { store => 
      
    val f = store.getObject(message.objectPointer)
                                 
    f foreach {
      result => 
        val response = result match {
          case Left(err) => err match {
            case e: InvalidLocalPointer => Left(ReadError.InvalidLocalPointer)
            case e: ObjectMismatch => Left(ReadError.ObjectMismatch)
            case e: CorruptedObject => Left(ReadError.CorruptedObject)
          }
          
          case Right((cs, data)) => Right((ReadResponse.CurrentState(cs.revision, cs.refcount, if (message.returnObjectData) Some(data) else None,
                                                                     if (message.returnLockedTransaction) cs.lockedTransaction else None), data))
        }
        
        response match {
          case Left(err) => messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Left(err)), None)
          case Right((state, data)) => messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Right(state)), Some(data))
        }
    }
  }
  
}