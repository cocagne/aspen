package com.ibm.aspen.base.impl

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.data_store.{DataStore, DataStoreID}
import com.ibm.aspen.core.network.{StoreSideAllocationMessageReceiver, StoreSideNetwork, StoreSideTransactionMessageReceiver}
import com.ibm.aspen.core.transaction.{LocalUpdate, TxFinalized, TxResolved, Message => TransactionMessage}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/** Represents a storage node that hosts multiple DataStore instances.
 *  
 *  Immediately upon creation the node registers itself with the network layer to support read
 *  operations. At a later time the recoverPendingOperations() method may be used to begin the
 *  transaction/allocation recovery process and place the node into a fully-operational mode. 
 * 
 */
class StorageNode(
    val system: AspenSystem,
    val crl: CrashRecoveryLog,
    val net: StoreSideNetwork,
    )(implicit ec: ExecutionContext) extends StoreSideTransactionMessageReceiver with StoreSideAllocationMessageReceiver {
  
  private[this] var stores = Map[DataStoreID, DataStore]()
  
  val readManager = new StorageNodeReadManager(net.readHandler)
  
  net.readHandler.setReceiver(readManager)
  
  readManager.hostedStores.foreach(store => net.registerHostedStore(store.storeId))
  
  private[this] var recoveredOption: Option[Recovered] = None
  
  private[this] def recovered = synchronized { recoveredOption }
  
  private case class Recovered(
    transactionManager: StorageNodeTransactionManager,
    allocationManager: StorageNodeAllocationManager) 
    
  private[this] val missedUpdatePoller = BackgroundTask.schedulePeriodic(Duration(15, SECONDS), callNow=false) {
    val ssnap = synchronized { stores }
    ssnap.values.foreach( _.pollAndRepairMissedUpdates(system) )
  }

  def idle: Future[Unit] =  synchronized {
    recoveredOption match {
      case None => Future.unit
      case Some(r) => Future.sequence(r.transactionManager.idle :: r.allocationManager.idle :: Nil).map(_=>())
    }
  }
    
  def shutdown(): Future[Unit] = synchronized {
    missedUpdatePoller.cancel()
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
      synchronized {
        stores += (store.storeId -> store)
      }
      store
    }
  }
}