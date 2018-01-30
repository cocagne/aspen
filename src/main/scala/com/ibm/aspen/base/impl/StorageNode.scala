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
import com.ibm.aspen.core.transaction.{Message => TransactionMessage}
import java.util.UUID
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.allocation.AllocateResponse
import com.ibm.aspen.core.allocation.AllocationErrors
import com.ibm.aspen.core.network.StoreSideNetwork
import com.ibm.aspen.core.network.StoreSideAllocationMessageReceiver
import com.ibm.aspen.core.allocation.AllocationStatusRequest
import com.ibm.aspen.core.allocation.AllocationStatusReply

/** Represents a storage node that hosts multiple DataStore instances.
 *  
 *  Immediately upon creation the node registers itself with the network layer to support read
 *  operations. At a later time the recoverPendingOperations() method may be used to begin the
 *  transaction/allocation recovery process and place the node into a fully-operational mode. 
 * 
 */
class StorageNode(
    val crl: CrashRecoveryLog,
    val net: StoreSideNetwork,
    )(implicit ec: ExecutionContext) extends StoreSideTransactionMessageReceiver with StoreSideAllocationMessageReceiver {
  
  val readManager = new StorageNodeReadManager(net.readHandler)
  
  net.readHandler.setReceiver(readManager)
  
  readManager.hostedStores.foreach(store => net.registerHostedStore(store.storeId))
  
  private[this] var recoveredOption: Option[Recovered] = None
  
  private[this] def recovered = synchronized { recoveredOption }
  
  private case class Recovered(
    transactionManager: StorageNodeTransactionManager,
    allocationManager: StorageNodeAllocationManager) 
    
  def shutdown(): Future[Unit] = synchronized {
    recoveredOption match {
      case None => Future.successful(())
      case Some(r) => 
        r.allocationManager.shutdown()
        r.transactionManager.shutdown()
    }
  }
  
  def recoverPendingOperations(txMgr: StorageNodeTransactionManager, allocMgr:StorageNodeAllocationManager): Unit = synchronized {
        
    if (recoveredOption.isEmpty) {
      
      val r = Recovered(txMgr, allocMgr)
      
      recoveredOption = Some(r)
      
      // Need to intercept transaction & allocation messages in order to handle some cross-manager functionality.
      // We'll forward the messages as appropriate
      net.transactionHandler.setReceiver(this)
      net.allocationHandler.setReceiver(this)
    
      readManager.hostedStores.foreach { store =>
        r.transactionManager.addStore(store)
        r.allocationManager.addStore(store)
      }
    }
  } 
  
  /** (unit test only) returns true if all transactions are complete */
  def allTransactionsComplete: Boolean = recovered match {
    case None => true
    case Some(r) => r.transactionManager.allTransactionsComplete
  }
  
  override def receive(message: Allocate): Unit = recovered.foreach(r => r.allocationManager.receive(message))
  
  override def receive(message: AllocationStatusRequest): Unit = recovered.foreach { r =>
    val status = r.transactionManager.getTransactionStatus(message.to, message.allocationTransactionUUID)
    r.allocationManager.receive(message, status)
  }
  override def receive(message: AllocationStatusReply): Unit = recovered.foreach(r => r.allocationManager.receive(message))
    
  override def receive(message: TransactionMessage, updateContent: Option[List[LocalUpdate]]): Unit = recovered.foreach { r =>
    message match {
      case m: TxResolved => r.allocationManager.receive(m)
      case m: TxFinalized => r.allocationManager.receive(m)
      case _ =>
    }
    r.transactionManager.receive(message, updateContent)
  }
    
  def addStore(storeId: DataStoreID, factory: DataStore.Factory): Future[DataStore] = { 
    val ltrs = crl.getTransactionRecoveryStateForStore(storeId)
    val lars = crl.getAllocationRecoveryStateForStore(storeId)
    
    factory(storeId, ltrs, lars) map { case store => 
      readManager.addStore(store)
      net.registerHostedStore(store.storeId)
      recovered foreach { r =>
        r.transactionManager.addStore(store)
        r.allocationManager.addStore(store)
      }
      store
    }
  }
}