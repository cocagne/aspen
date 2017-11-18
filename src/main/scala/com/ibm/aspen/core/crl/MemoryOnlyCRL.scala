package com.ibm.aspen.core.crl

import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import scala.concurrent.Future
import scala.concurrent.Promise
import java.nio.ByteBuffer
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.allocation.AllocationRecoveryState

object MemoryOnlyCRL extends CrashRecoveryLog {
  private [this] var pendingTransactions = Map[UUID, TransactionRecoveryState]()
  private [this] var pendingAllocations = Map[UUID, AllocationRecoveryState]()
  
  private val queue = new java.util.concurrent.LinkedBlockingQueue[Promise[Unit]]()
  
  override def close(): Future[Unit] = Future.successful(())
  
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
  
  override def getFullAllocationRecoveryState(): Map[DataStoreID, List[AllocationRecoveryState]] = synchronized {
    var m = Map[DataStoreID, List[AllocationRecoveryState]]()
    pendingAllocations.valuesIterator.foreach(ars => m.get(ars.storeId) match {
      case None => m += (ars.storeId -> List(ars))
      case Some(l) =>
        val newList = ars :: l
        m += (ars.storeId -> newList)
    })
    m
  }
  
  override def getAllocationRecoveryStateForStore(storeId: DataStoreID): List[AllocationRecoveryState] = synchronized {
    pendingAllocations.foldLeft(List[AllocationRecoveryState]())( (l, t) => if (t._2.storeId == storeId) t._2 :: l else l )
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
  
  override def saveAllocationRecoveryState(state: AllocationRecoveryState): Future[Unit] = synchronized {
    pendingAllocations += (state.allocationTransactionUUID -> state)
    val p = Promise[Unit]()
    queue.put(p)
    p.future
  }
  
  override def discardAllocationState(allocationTransactionUUID: UUID): Unit = synchronized {
    pendingAllocations -= allocationTransactionUUID
  }
  
  private val backgroundThread = new Thread("MemoryOnlyCRL") {
    override def run(): Unit = while(true) queue.take().success(())
  }
  
  backgroundThread.setDaemon(true)
  backgroundThread.start()
}