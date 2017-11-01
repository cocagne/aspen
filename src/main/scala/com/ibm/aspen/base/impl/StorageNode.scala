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

class StorageNode(
  val crl: CrashRecoveryLog, 
  val messenger: StorageNodeMessenger,
  val driverFactory: TransactionDriver.Factory,
  val finalizerFactory: TransactionFinalizer.Factory,
  val initialStores: List[DataStore],
  val onStoreInitializationFailure: (DataStore, Throwable) => Unit
)(implicit ec: ExecutionContext) extends StoreSideTransactionMessageReceiver with StoreSideReadMessageReceiver with StoreSideAllocationMessageReceiver {
  
  private[this] var stores = Map[DataStoreID, DataStore]()
  private[this] val txManager = new StorageNodeTransactionManager(crl, messenger, driverFactory, finalizerFactory, getStore)
  
  private def getStore(sid: DataStoreID) = synchronized { stores.get(sid) }
  
  /** Waits for the store to complete its internal initialization then adds it to the map of active stores */
  private def initializeStore(store: DataStore, transactionRecoveryStates: List[TransactionRecoveryState]): Future[Unit] = {
    store.initialize(transactionRecoveryStates) andThen {
      case Success(_) => synchronized { stores += (store.storeId -> store) }
      case Failure(cause) => onStoreInitializationFailure(store, cause)
    }
  }
  
  /** Completes when all initialStores are fully initialized and ready for use */
  val initialized: Future[Unit] = {
    val pNetworkReady = Promise[Unit]()
    val txRecoveryState = crl.getFullTransactionRecoveryState()
    
    def initStore(ds: DataStore): Future[Unit] = initializeStore(ds, txRecoveryState.getOrElse(ds.storeId, Nil))
    
    for {
      storesInitialized <- Future.sequence(initialStores.map(initStore)).map(_=>())
      txMgrReady <- txManager.recoverTransactions(txRecoveryState, pNetworkReady.future)
      networkReady <- messenger.initialize(this) 
    } yield {
      pNetworkReady.success(())
      ()
    }
  }
  
  def receive(m: Allocate): Unit = getStore(m.toStore).foreach(store => {
    val f = store.allocateNewObject(m.newObjectUUID, m.objectSize, m.objectData, m.initialRefcount, 
                                    m.allocationTransactionUUID, m.allocatingObject, m.allocatingObjectRevision)
    // Failure is communicated by the result not a failed future
    f onSuccess {  case result => 
      messenger.send(m.fromClient, allocation.AllocateResponse(m.toStore, m.allocationTransactionUUID, result)) 
    }
  })
  
  def receive(message: transaction.Message, updateContent: Option[Array[ByteBuffer]]): Unit  = {
    txManager.receive(message, updateContent)
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
          case Left(err) => messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Left(err)), None)
          case Right((state, data)) => messenger.send(message.fromClient, ReadResponse(message.toStore, message.readUUID, Right(state)), Some(data))
        }
    }
  })
}