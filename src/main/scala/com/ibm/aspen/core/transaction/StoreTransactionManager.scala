package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.DataStore
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.network.StoreSideTransactionMessageReceiver
import com.github.blemale.scaffeine.Scaffeine
import scala.concurrent.duration._


class StoreTransactionManager(
    val crl: CrashRecoveryLog, 
    val messenger: StoreSideTransactionMessenger,
    val driverFactory: TransactionDriver.Factory,
    val finalizerFactory: TransactionFinalizer.Factory)(implicit ec: ExecutionContext) extends StoreSideTransactionMessageReceiver {
 
  private[this] var stores = Map[DataStoreID, DataStore]()
  private[this] var transactions = Map[UUID, Transaction]()
  private[this] var transactionDrivers = Map[UUID, TransactionDriver]()
  
  private[this] def getTransaction(txuuid: UUID): Option[Transaction] = synchronized { transactions.get(txuuid) }
  
  private[this] def getTransactionDriver(txuuid: UUID): Option[TransactionDriver] = synchronized { transactionDrivers.get(txuuid) }
  
  private[this] def getStore(id: DataStoreID): Option[DataStore] = synchronized { stores.get(id) }
  private[this] def addStore(store: DataStore): Unit = synchronized { stores += (store.storeId -> store) }
  
  private[this] val prepareResponseCache = Scaffeine().recordStats()
                                                      .expireAfterWrite(10.seconds)
                                                      .maximumSize(1000)
                                                      .build[UUID, Map[DataStoreID,TxPrepareResponse]]()
  
  private def onTransactionDiscarded(t: Transaction) = synchronized { transactions -= t.txd.transactionUUID }
  
  private def onTransactionDriverComplete(txuuid: UUID) = synchronized { transactionDrivers -= txuuid }
  
  def receive(toStore: DataStoreID, message: Message, updateContent: Option[LocalUpdateContent]): Unit = getStore(toStore) foreach ( 
    store => {
      message match {
      case m: TxPrepare => 
        val tx = synchronized {
          transactions.get(m.txd.transactionUUID).getOrElse({
            val t = Transaction(crl, messenger, onTransactionDiscarded _, store, m.txd, updateContent.getOrElse(new MissingUpdateContent))
            transactions += (m.txd.transactionUUID -> t)
            
            if (m.txd.designatedLeaderUID == store.storeId.poolIndex) {
              val driver = driverFactory.create(toStore, messenger, m, finalizerFactory, onTransactionDriverComplete _)
              transactionDrivers += (m.txd.transactionUUID -> driver)
              
              // If any TxPrepareResponse messages were received before we noticed that we're the transaction driver, process them now
              prepareResponseCache.getIfPresent(m.txd.transactionUUID).foreach( pmap => {
                pmap.foreach( t => driver.receiveTxPrepareResponse(t._2) )
              })
            }
            
            // No need to keep the cached entries around (if any) the driver will receive the messages directly
            prepareResponseCache.invalidate(m.txd.transactionUUID)
            t
          })
        }
        tx.receivePrepare(m)
        getTransactionDriver(m.txd.transactionUUID).foreach( td => td.receiveTxPrepare(m) )
   
      case m: TxPrepareResponse =>
        getTransactionDriver(m.transactionUUID) match {
          case Some(td) => td.receiveTxPrepareResponse(m)
          
          case None =>
            // TxPrepareResponse messages are unicast to the transaction leader. That we're receiving one probably means we're
            // the designated leader for the transaction. We'll hold on to these for a while so that if we receive the prepare
            // message, we can immediately process the replies rather than having to rely on the driver to recover the transaction
            // via retransmissions or another Paxos round.
            val pm = prepareResponseCache.getIfPresent(m.transactionUUID) match {
              case Some(pmap) => pmap
              case None => Map[DataStoreID,TxPrepareResponse]()
            }
            prepareResponseCache.put(m.transactionUUID, pm + (m.from -> m))
        }
      
      case m: TxAccept => getTransaction(m.transactionUUID).foreach( tx => tx.receiveAccept(m) )
      
      case m: TxAcceptResponse =>
        getTransaction(m.transactionUUID).foreach( tx => tx.receiveAcceptResponse(m) )
        getTransactionDriver(m.transactionUUID).foreach( td => td.receiveTxAcceptResponse(m) )
        
      case m: TxFinalized =>
        getTransaction(m.transactionUUID).foreach( tx => tx.receiveFinalized(m) )
        getTransactionDriver(m.transactionUUID).foreach( td => td.receiveTxFinalized(m) )
    }
  })
 
}
