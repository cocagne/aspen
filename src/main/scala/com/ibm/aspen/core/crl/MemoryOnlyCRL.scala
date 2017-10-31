package com.ibm.aspen.core.crl

import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import scala.concurrent.Future
import scala.concurrent.Promise
import java.nio.ByteBuffer
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID

object MemoryOnlyCRL extends CrashRecoveryLog {
  private [this] var pendingTransactions = Map[UUID, TransactionRecoveryState]()
  
  private val queue = new java.util.concurrent.LinkedBlockingQueue[Promise[Unit]]()
  
  override def getFullTransactionRecoveryState(): Map[DataStoreID, List[TransactionRecoveryState]] = synchronized {
    var m = Map[DataStoreID, List[TransactionRecoveryState]]()
    pendingTransactions.valuesIterator.foreach(trs => m.get(trs.storeId) match {
      case None => m += (trs.storeId -> List(trs))
      case Some(l) =>
        val newList = trs:: l
        m += (trs.storeId -> newList)
    })
    m
  }
  
  override def getTransactionRecoveryStateForStore(storeId: DataStoreID): List[TransactionRecoveryState] = synchronized {
    pendingTransactions.foldLeft(List[TransactionRecoveryState]())( (l, t) => if (t._2.storeId == storeId) t._2 :: l else l )
  }
  
  override def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = synchronized {
    pendingTransactions += (state.txd.transactionUUID -> state)
    val p = Promise[Unit]()
    queue.put(p)
    p.future
  }
  
  override def discardTransactionState(txd: TransactionDescription): Unit = synchronized {
    pendingTransactions -= txd.transactionUUID
  }
  
  private val backgroundThread = new Thread("MemoryOnlyCRL") {
    override def run(): Unit = while(true) queue.take().success(())
  }
  
  backgroundThread.setDaemon(true)
  backgroundThread.start()
}