package com.ibm.aspen.base.impl

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import com.ibm.aspen.core.transaction
import com.ibm.aspen.core.allocation
import com.ibm.aspen.core.transaction.TransactionDriver
import com.ibm.aspen.core.transaction.TransactionFinalizer
import com.ibm.aspen.core.network.StoreSideReadMessenger
import com.ibm.aspen.core.network.StoreSideAllocationMessenger
import com.ibm.aspen.core.network.StoreSideTransactionMessageReceiver
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.StoreSideReadMessageReceiver
import com.ibm.aspen.core.network.StoreSideAllocationMessageReceiver
import com.ibm.aspen.core.data_store.DataStore
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.data_store.ObjectError
import com.ibm.aspen.core.read.ReadError
import com.ibm.aspen.core.read.ReadResponse
import java.nio.ByteBuffer
import scala.concurrent.Future
import com.ibm.aspen.core.read.Read
import com.ibm.aspen.core.allocation.Allocate
import scala.util.Success
import scala.util.Failure
import scala.concurrent.Promise
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.data_store.InvalidLocalPointer
import com.ibm.aspen.core.data_store.ObjectMismatch
import com.ibm.aspen.core.data_store.CorruptedObject
import com.ibm.aspen.core.transaction.LocalUpdate
import java.util.UUID
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.allocation.StoreAllocationManager
import com.ibm.aspen.core.allocation.AllocateResponse
import com.ibm.aspen.core.allocation.AllocationErrors

// addStore(fact: DataStore.Factory): Future[Unit]
class StorageNode(
  val crl: CrashRecoveryLog, 
  val messenger: StorageNodeMessenger,
  private[this] val allocationManager: StoreAllocationManager,
  private[this] val transactionManager: StorageNodeTransactionManager
  )(implicit ec: ExecutionContext) extends StoreSideTransactionMessageReceiver with StoreSideReadMessageReceiver with StoreSideAllocationMessageReceiver {
  
  private[this] var stores = Map[DataStoreID, DataStore]()
  private[this] val networkInitialized = messenger.initialize(this)
  
  def clientId: ClientID = messenger.client
  
  private def getStore(sid: DataStoreID) = synchronized { stores.get(sid) }
  
  def addStore(storeId: DataStoreID, factory: DataStore.Factory): Future[DataStore] = networkInitialized.flatMap { case _ =>
    val ltrs = crl.getTransactionRecoveryStateForStore(storeId)
    val lars = crl.getAllocationRecoveryStateForStore(storeId)
    factory(storeId, allocationManager, ltrs, lars).initialized map { case store => 
      synchronized {
        stores += (storeId -> store)
        transactionManager.addStore(store, ltrs)
        lars.foreach(ars => allocationManager.trackAllocation(store, ars))
      }
      store
    }
  }
    
  /** (unit test only) returns true if all transactions are complete */
  def allTransactionsComplete: Boolean = transactionManager.allTransactionsComplete
  
  def receive(m: Allocate): Unit = getStore(m.toStore).foreach{ store => {
    
    def reply(result: Either[AllocationErrors.Value, List[AllocateResponse.Allocated]]) = {
      messenger.send(m.fromClient, allocation.AllocateResponse(m.toStore, m.allocationTransactionUUID, result))
    }
    
    store.allocate(
        m.newObjects, 
        m.allocationTransactionUUID, 
        m.allocatingObject, 
        m.allocatingObjectRevision).foreach { r => r match {
          
        case Left(err) => reply(Left(err))
        
        case Right(ars) => 
          allocationManager.trackAllocation(store, ars) foreach { _ =>
            val allocs = ars.newObjects.map(n => AllocateResponse.Allocated(n.newObjectUUID, n.storePointer)) 
            reply(Right(allocs))
          }
      }
    }
  }}
  
  def receive(message: transaction.Message, updateContent: Option[List[LocalUpdate]]): Unit  = {
    transactionManager.receive(message, updateContent)
    
    message match {
      case m: TxResolved => allocationManager.receive(m)
      case m: TxFinalized => allocationManager.receive(m)
      case _ =>
    }
  }
  
  def receive(message: Read): Unit = getStore(message.toStore).foreach(store => {
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
  })
}