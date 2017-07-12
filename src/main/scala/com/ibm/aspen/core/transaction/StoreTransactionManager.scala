package com.ibm.aspen.core.transaction

import com.ibm.aspen.core.crl.CrashRecoveryLog
import com.ibm.aspen.core.network.StoreSideTransactionMessenger
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.DataStore
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.network.TransactionMessageReceiver


class StoreTransactionManager(
    val crl: CrashRecoveryLog, 
    val messenger: StoreSideTransactionMessenger,
    val driverFactory: TransactionDriver.Factory,
    val finalizerFactory: TransactionFinalizer.Factory)(implicit ec: ExecutionContext) extends TransactionMessageReceiver {
 
  private[this] var stores = Map[DataStoreID, DataStore]()
  private[this] var transactions = Map[UUID, Transaction]()
  private[this] var transactionDrivers = Map[UUID, TransactionDriver]()
  
  private[this] def getTransaction(txuuid: UUID): Option[Transaction] = synchronized { transactions.get(txuuid) }
  
  private[this] def getTransactionDriver(txuuid: UUID): Option[TransactionDriver] = synchronized { transactionDrivers.get(txuuid) }
  
  private[this] def getStore(id: DataStoreID): Option[DataStore] = synchronized { stores.get(id) }
  private[this] def addStore(store: DataStore): Unit = synchronized { stores += (store.storeId -> store) }
  
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
            if (m.txd.designatedLeaderUID == store.storeId.poolIndex)
              transactionDrivers += (m.txd.transactionUUID -> driverFactory.create(toStore, messenger, m, finalizerFactory, onTransactionDriverComplete _))
            t
          })
        }
        tx.receivePrepare(m)
        getTransactionDriver(m.txd.transactionUUID).foreach( td => td.receiveTxPrepare(m) )
   
      case m: TxPrepareResponse =>
        // TODO - Handle PrepareResponse messages seen before corresponding Prepare message is seen
        //        Will likely involve caching recently-seen transaction UUIDs
        //        Fixed-size cache of 1000 entries or so should be sufficient
        getTransactionDriver(m.transactionUUID).foreach( td => td.receiveTxPrepareResponse(m) )
      
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
