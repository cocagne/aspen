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
  private [this] var pendingTransactions = Map[DataStoreID, Map[UUID, TransactionRecoveryState]]()
  private [this] var pendingAllocations = Map[DataStoreID, Map[UUID, AllocationRecoveryState]]()
  
  private val queue = new java.util.concurrent.LinkedBlockingQueue[Promise[Unit]]()
  
  override def close(): Future[Unit] = Future.successful(())
  
  private def gT(storeId: DataStoreID): Map[UUID, TransactionRecoveryState] = pendingTransactions.get(storeId) match {
    case Some(m) => m
    case None =>
      val m = Map[UUID, TransactionRecoveryState]()
      pendingTransactions = pendingTransactions + (storeId -> m)
      m
  }
  
  private def gA(storeId: DataStoreID): Map[UUID, AllocationRecoveryState] = pendingAllocations.get(storeId) match {
    case Some(m) => m
    case None =>
      val m = Map[UUID, AllocationRecoveryState]()
      pendingAllocations = pendingAllocations + (storeId -> m)
      m
  }
  
  override def getFullTransactionRecoveryState(): Map[DataStoreID, List[TransactionRecoveryState]] = synchronized {
    var m = Map[DataStoreID, List[TransactionRecoveryState]]()
    pendingTransactions.foreach(t => { t._2.valuesIterator.foreach(trs => m.get(trs.storeId) match {
      case None => m += (trs.storeId -> List(trs))
      case Some(l) =>
        val newList = trs:: l
        m += (trs.storeId -> newList)
      })
    })
    m
  }
  
  override def getTransactionRecoveryStateForStore(storeId: DataStoreID): List[TransactionRecoveryState] = synchronized {
    val m = pendingTransactions.getOrElse(storeId, Map[UUID, TransactionRecoveryState]())
    m.values.toList
  }
  
  override def getFullAllocationRecoveryState(): Map[DataStoreID, List[AllocationRecoveryState]] = synchronized {
    var m = Map[DataStoreID, List[AllocationRecoveryState]]()
    pendingAllocations.foreach(t => { t._2.valuesIterator.foreach(ars => m.get(ars.storeId) match {
      case None => m += (ars.storeId -> List(ars))
      case Some(l) =>
        val newList = ars:: l
        m += (ars.storeId -> newList)
      })
    })
    m
  }
  
  override def getAllocationRecoveryStateForStore(storeId: DataStoreID): List[AllocationRecoveryState] = synchronized {
    val m = pendingAllocations.getOrElse(storeId, Map[UUID, AllocationRecoveryState]())
    m.values.toList
  }
  
  override def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = synchronized {
    val ins = (state.txd.transactionUUID -> state)
    val smap = gT(state.storeId)
    
    pendingTransactions = pendingTransactions + (state.storeId -> (smap + ins))
    
    val p = Promise[Unit]()
    queue.put(p)
    p.future
  }
  
  override def discardTransactionState(storeId: DataStoreID, txd: TransactionDescription): Unit = synchronized {
    val smap = gT(storeId)
    pendingTransactions = pendingTransactions + (storeId -> (smap - txd.transactionUUID))
  }
  
  override def saveAllocationRecoveryState(state: AllocationRecoveryState): Future[Unit] = synchronized {
    val ins = (state.allocationTransactionUUID -> state)
    val smap = gA(state.storeId)
    
    pendingAllocations = pendingAllocations + (state.storeId -> (smap + ins))
    
    val p = Promise[Unit]()
    queue.put(p)
    p.future
  }
  
  override def discardAllocationState(storeId: DataStoreID, allocationTransactionUUID: UUID): Unit = synchronized {
    val smap = gA(storeId)
    pendingAllocations = pendingAllocations + (storeId -> (smap - allocationTransactionUUID))
  }
  
  private val backgroundThread = new Thread("MemoryOnlyCRL") {
    override def run(): Unit = while(true) queue.take().success(())
  }
  
  backgroundThread.setDaemon(true)
  backgroundThread.start()
}