package com.ibm.aspen.base.impl

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.DataStore
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.network.StoreSideTransactionMessageReceiver
import com.github.blemale.scaffeine.Scaffeine
import scala.concurrent.duration._
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.Message
import com.ibm.aspen.core.transaction.Transaction
import com.ibm.aspen.core.transaction.TransactionDriver
import com.ibm.aspen.core.transaction.TransactionFinalizer
import com.ibm.aspen.core.transaction.TxAccept
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.transaction.TxPrepareResponse
import scala.annotation.implicitNotFound
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import scala.concurrent.Future
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.transaction.TxHeartbeat
import com.ibm.aspen.core.transaction.TxStatusRequest
import com.ibm.aspen.core.transaction.TxStatusReply
import com.ibm.aspen.core.transaction.TransactionDescription


class StorageNodeTransactionManager(
    val crl: CrashRecoveryLog, 
    val messenger: StoreSideTransactionMessenger,
    val driverFactory: TransactionDriver.Factory,
    val finalizerFactory: TransactionFinalizer.Factory)(implicit ec: ExecutionContext) extends StoreSideTransactionMessageReceiver {
  
  private[this] var stores = Map[DataStoreID, StoreState]()
 
  private[this] val prepareResponseCache = Scaffeine().expireAfterWrite(10.seconds)
                                                      .maximumSize(1000)
                                                      .build[UUID, List[TxPrepareResponse]]()
                                                      
  private[this] val resultCache = Scaffeine().maximumSize(1000)
                                             .build[UUID, Boolean]()
  
  def addStore(store: DataStore): Unit = synchronized {
    val ltrs = crl.getTransactionRecoveryStateForStore(store.storeId)
    
    stores += (store.storeId -> new StoreState(store, ltrs))
  }
  
  def removeStore(storeId: DataStoreID): Unit = synchronized {
    stores -= storeId
  }
  
  private[this] def getStore(storeId: DataStoreID): Option[StoreState] = synchronized {
    stores.get(storeId)
  }
  
  protected def getStores(): Map[DataStoreID, StoreState] = synchronized { stores }
  
  private def cachePrepareResponse(m: TxPrepareResponse): Unit = synchronized {
    // Do this in a synchronized block to serialize updates to the cached list
    val l = prepareResponseCache.getIfPresent(m.transactionUUID) match {
      case Some(l) => l
      case None => Nil
    }
    prepareResponseCache.put(m.transactionUUID, m :: l)
  }
  
  /** (unit test only) returns true if all transactions are complete */
  def allTransactionsComplete: Boolean = synchronized { 
    stores.forall(t => t._2.allTransactionsComplete)
  }
  
  def receive(message: Message, updateContent: Option[List[LocalUpdate]]): Unit = {
    getStore(message.to).foreach(ss => ss.receive(message, updateContent))
  }
  
  protected class StoreState(val store: DataStore, txRecoveryState: List[TransactionRecoveryState]) {
    
    private[this] var transactionDrivers = Map[UUID, TransactionDriver]()
    private[this] var transactions: Map[UUID, Transaction] = txRecoveryState.map { trs => 
      (trs.txd.transactionUUID -> Transaction(crl, messenger, onTransactionDiscarded _, store, trs.txd, trs.localUpdates))
    }.toMap
    
    def allTransactionsComplete: Boolean = synchronized { transactions.isEmpty }
    
    def getTransaction(txUUID: UUID) = synchronized { transactions.get(txUUID) }
    
    def getTransactionDriver(txUUID: UUID) = synchronized { transactionDrivers.get(txUUID) }
    
    def getTransactions(): (Map[UUID, Transaction], Map[UUID, TransactionDriver]) = synchronized { 
      (transactions, transactionDrivers) 
    }
    
    def onTransactionDiscarded(t: Transaction) = synchronized { transactions -= t.txd.transactionUUID }
  
    def onTransactionDriverComplete(txuuid: UUID) = synchronized { transactionDrivers -= txuuid }
    
    def driveTransaction(txd: TransactionDescription): TransactionDriver = synchronized {
      val driver = driverFactory.create(store.storeId, messenger, txd, finalizerFactory, onTransactionDriverComplete _)
      
      transactionDrivers += (txd.transactionUUID -> driver)
      
      driver
    }
    
    def receive(message: Message, updateContent: Option[List[LocalUpdate]]): Unit = {
      //println(s"Store RCV: to ${message.to} from ${message.from} class ${message.getClass}")
      message match {
        case m: TxPrepare =>  
          val (tx, driver) = synchronized {
            val tx = transactions.get(m.txd.transactionUUID).getOrElse {
            
              val t = Transaction(crl, messenger, onTransactionDiscarded _, store, m.txd, updateContent)
              
              transactions += (m.txd.transactionUUID -> t)
              
              if (m.txd.designatedLeaderUID == store.storeId.poolIndex) {
                val driver = driveTransaction(m.txd)
                
                // If any TxPrepareResponse messages were received before we noticed that we're the transaction driver, process them now
                prepareResponseCache.getIfPresent(m.txd.transactionUUID).foreach { lst => 
                  lst.foreach(driver.receiveTxPrepareResponse)
                  
                  // No need to keep the cached entries around
                  prepareResponseCache.invalidate(m.txd.transactionUUID)
                }
              }
              
              t
            }
            
            (tx, transactionDrivers.get(m.txd.transactionUUID))
          }
          
          tx.receivePrepare(m, updateContent)
          driver.foreach(td => td.receiveTxPrepare(m)) 
          
     
        case m: TxPrepareResponse => 
          getTransactionDriver(m.transactionUUID)  match {
            case Some(td) => td.receiveTxPrepareResponse(m)
            
            case None =>
              // TxPrepareResponse messages are unicast to the transaction leader. That we're receiving one probably means we're
              // the designated leader for the transaction that either recently completed or we haven't discovered yet. If it's not
              // in the result cache, We'll hold on to these for a while so that if we receive the prepare message, we can immediately
              // process the replies rather than having to rely on the driver to recover the transaction via retransmissions or 
              // another Paxos round.
              resultCache.getIfPresent(m.transactionUUID) match {
                case Some(result) => messenger.send(TxResolved(m.from, store.storeId, m.transactionUUID, result))
                
                case None => cachePrepareResponse(m)
              }              
          }
        
        case m: TxAccept => 
          getTransaction(m.transactionUUID).foreach( _.receiveAccept(m) )
        
        case m: TxAcceptResponse =>
          getTransaction(m.transactionUUID).foreach( _.receiveAcceptResponse(m) )
          getTransactionDriver(m.transactionUUID).foreach( _.receiveTxAcceptResponse(m) )
          
        case m: TxResolved =>
          resultCache.put(m.transactionUUID, m.committed)
          getTransaction(m.transactionUUID).foreach( _.receiveResolved(m) )
          getTransactionDriver(m.transactionUUID).foreach( _.receiveTxResolved(m) )
          
        case m: TxFinalized =>
          resultCache.put(m.transactionUUID, m.committed)
          getTransaction(m.transactionUUID).foreach( _.receiveFinalized(m) )
          getTransactionDriver(m.transactionUUID).foreach( _.receiveTxFinalized(m) )
          
        case m: TxHeartbeat => getTransaction(m.transactionUUID).foreach( _.heartbeatReceived() )
        
        case m: TxStatusRequest => // TODO
          
        case m: TxStatusReply => // TODO
      }
    }
  }
 
}
