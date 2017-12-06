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
    )(implicit ec: ExecutionContext) extends StoreSideTransactionMessageReceiver {
  
  val readManager = new StorageNodeReadManager(net.readHandler)
  
  net.readHandler.setReceiver(readManager)
  
  readManager.hostedStores.foreach(store => net.registerHostedStore(store.storeId))
  
  private[this] var recoveredOption: Option[Recovered] = None
  
  private[this] def recovered = synchronized { recoveredOption }
  
  private case class Recovered(
    transactionManager: StorageNodeTransactionManager,
    allocationManager: StorageNodeAllocationManager) 
  
  def recoverPendingOperations(
      txDriverFactory: TransactionDriver.Factory,
      txFinalizerFactory: TransactionFinalizer.Factory): Unit = synchronized {
        
    if (recoveredOption.isEmpty) {
      
      val r = Recovered(
          new StorageNodeTransactionManager(crl, net.transactionHandler, txDriverFactory, txFinalizerFactory),
          new StorageNodeAllocationManager(crl, net.allocationHandler))
      
      recoveredOption = Some(r)
      
      // Need to intercept transaction messages in order to forward some on to the allocation manager so
      // we'll set ourself as the receiver
      net.transactionHandler.setReceiver(this)
      
      net.allocationHandler.setReceiver(r.allocationManager)
    
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
    
  def receive(message: TransactionMessage, updateContent: Option[List[LocalUpdate]]): Unit = recovered.foreach { r =>
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