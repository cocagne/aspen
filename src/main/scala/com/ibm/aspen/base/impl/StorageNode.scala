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
import com.ibm.aspen.core.transaction.StoreTransactionManager
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

class StorageNode(
  val crl: CrashRecoveryLog, 
  val transactionMessenger: StoreSideTransactionMessenger,
  val readMessenger: StoreSideReadMessenger,
  val allocationMessenger: StoreSideAllocationMessenger,
  val driverFactory: TransactionDriver.Factory,
  val finalizerFactory: TransactionFinalizer.Factory,
  val initialStores: List[DataStore]
)(implicit ec: ExecutionContext) extends StoreSideTransactionMessageReceiver with StoreSideReadMessageReceiver with StoreSideAllocationMessageReceiver {
  
  private[this] var stores = Map[DataStoreID, DataStore]()
  private[this] val txManager = new StoreTransactionManager(crl, transactionMessenger, driverFactory, finalizerFactory)
  
  private[this] def getStore(sid: DataStoreID) = synchronized { stores.get(sid) }
  
  /** Waits for the store to complete its internal initialization then adds it to the map of active stores */
  def addStore(store: DataStore): Future[Unit] = store.initialized andThen {
    case _ => synchronized { stores += (store.storeId -> store) }
  }
  
  /** Completes when all initialStores are fully initialized */
  val initialized: Future[Unit] = Future.sequence(initialStores.map(addStore)).map(_=>())
  
  def receive(message: Allocate): Unit = message match {
    case m: allocation.Allocate => getStore(m.toStore).foreach(store => {
      val f = store.allocateNewObject(m.newObjectUUID, m.objectSize, m.objectData, m.initialRefcount, 
                                      m.allocationTransactionUUID, m.allocatingObject, m.allocatingObjectRevision)
      f onSuccess {  case result => 
        allocationMessenger.send(m.fromClient, allocation.AllocateResponse(m.toStore, m.allocationTransactionUUID, result)) 
      }
    })
    case _ => // Ignore other allocation messages
  }
  
  def receive(fromStore: DataStoreID, message: transaction.Message, updateContent: Option[Array[ByteBuffer]]): Unit  = {
    txManager.receive(fromStore, message, updateContent)
  }
  
  def receive(message: Read): Unit = getStore(message.toStore).foreach(store => {
    val f = store.getObject(message.objectPointer)
                                 
    f onSuccess {
      case result => 
        val response = result match {
          case Left(err) => err match {
            case ObjectError.InvalidLocalPointer => Left(ReadError.InvalidLocalPointer)
            case ObjectError.ObjectMismatch => Left(ReadError.ObjectMismatch)
            case ObjectError.CorruptedObject => Left(ReadError.CorruptedObject)
            //
            // The following two should not be possible for a simple read since we're not checking versions/counts
            // This probably means we should break out object read errors from object check errors
            //
            case ObjectError.RefcountMismatch => Left(ReadError.UnexpectedInternalError)
            case ObjectError.RevisionMismatch => Left(ReadError.UnexpectedInternalError)
          }
          
          case Right((cs, data)) => Right((ReadResponse.CurrentState(cs.revision, cs.refcount, if (message.returnObjectData) Some(data) else None,
                                                                    if (message.returnLockedTransaction) cs.lockedTransaction else None), data))
        }
        
        response match {
          case Left(err) => readMessenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Left(err)), None)
          case Right((state, data)) => readMessenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Right(state)), Some(data))
        }
    }
  })
}