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
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import java.util.UUID
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.core.data_store.ObjectReadError

class StorageNodeReadManager(messenger: StoreSideReadMessenger)(implicit ec: ExecutionContext) extends StoreSideReadMessageReceiver {
  
  private[this] var stores = Map[DataStoreID, DataStore]()
  
  private def getStore(sid: DataStoreID) = synchronized { stores.get(sid) }
  
  def hostedStores: List[DataStore] = synchronized { stores.values.toList }
  
  def addStore(store: DataStore): Unit = synchronized { stores += (store.storeId -> store) }
      
  def receive(message: Read): Unit = getStore(message.toStore).foreach { store => 
    
    def cvt(err: ObjectReadError): ReadError.Value = err match {
      case e: InvalidLocalPointer => ReadError.InvalidLocalPointer
      case e: ObjectMismatch => ReadError.ObjectMismatch
      case e: CorruptedObject => ReadError.CorruptedObject
    }
    
    val readData = message.returnObjectData || (message.objectPointer match {
      case _ : KeyValueObjectPointer => true
      case _ => false
    })
    
    if (readData) {
      store.getObject(message.objectPointer) foreach { result => result match { 
        case Left(err) => messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Left(cvt(err))))
        
        case Right((md, data, locks)) => 
          val updates = message.objectPointer match {
            case _: KeyValueObjectPointer => 
              if (message.returnObjectData)
                Set[UUID]()
              else 
                KeyValueObjectCodec.getUpdateSet(data)
                
            case _ => Set[UUID]()
          }
          
          val cs = ReadResponse.CurrentState(md.revision, updates, md.refcount, md.timestamp, 
              if (message.returnObjectData) Some(data) else None,
              if (message.returnLockedTransaction) locks else Nil)
              
          messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Right(cs)))
       }}
    } else {
      store.getObjectMetadata(message.objectPointer) foreach { result => result match { 
        case Left(err) => messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Left(cvt(err))))
        
        case Right((md, locks)) => 

          val cs = ReadResponse.CurrentState(md.revision, Set[UUID](), md.refcount, md.timestamp, None,
              if (message.returnLockedTransaction) locks else Nil)
              
          messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Right(cs)))
       }}
    }
  }
  
}