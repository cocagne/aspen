package com.ibm.aspen.core.crl

import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import scala.concurrent.Future
import scala.concurrent.Promise
import java.nio.ByteBuffer
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.allocation.AllocationRecoveryState

class MemoryOnlyCRL extends CrashRecoveryLog {
  private [this] var pendingTransactions = Map[DataStoreID, Map[UUID, TransactionRecoveryState]]()
  private [this] var pendingAllocations = Map[DataStoreID, Map[UUID, List[AllocationRecoveryState]]]()
  
  override def close(): Future[Unit] = Future.successful(())
  
  private def gT(storeId: DataStoreID): Map[UUID, TransactionRecoveryState] = pendingTransactions.get(storeId) match {
    case Some(m) => m
    case None =>
      val m = Map[UUID, TransactionRecoveryState]()
      pendingTransactions = pendingTransactions + (storeId -> m)
      m
  }
  
  private def gA(storeId: DataStoreID): Map[UUID, List[AllocationRecoveryState]] = pendingAllocations.get(storeId) match {
    case Some(m) => m
    case None =>
      val m = Map[UUID, List[AllocationRecoveryState]]()
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
    pendingAllocations.foreach(t => { t._2.valuesIterator.foreach { lars => lars.foreach { ars => 
        m.get(ars.storeId) match {
          case None => m += (ars.storeId -> List(ars))
          case Some(l) =>
            val newList = ars:: l
            m += (ars.storeId -> newList)
        }
      }
    }
    })
    m
  }
  
  override def getAllocationRecoveryStateForStore(storeId: DataStoreID): List[AllocationRecoveryState] = synchronized {
    val m = pendingAllocations.getOrElse(storeId, Map[UUID, List[AllocationRecoveryState]]())
    m.values.toList.flatten
  }
  
  override def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = synchronized {
    val ins = (state.txd.transactionUUID -> state)
    val smap = gT(state.storeId)
    
    pendingTransactions = pendingTransactions + (state.storeId -> (smap + ins))
    
    Future.successful(())
  }
  
  override def discardTransactionState(storeId: DataStoreID, txd: TransactionDescription): Unit = synchronized {
    val smap = gT(storeId)
    pendingTransactions = pendingTransactions + (storeId -> (smap - txd.transactionUUID))
  }
  
  override def saveAllocationRecoveryState(state: AllocationRecoveryState): Future[Unit] = synchronized {
    val smap = gA(state.storeId)
    val lst = state :: smap.getOrElse(state.allocationTransactionUUID, Nil)
    val updated = smap + (state.allocationTransactionUUID -> lst) 
    
    pendingAllocations = pendingAllocations + (state.storeId -> updated)
    
    Future.successful(())
  }
  
  override def discardAllocationState(state: AllocationRecoveryState): Unit = synchronized {
    val smap = gA(state.storeId)
    pendingAllocations = pendingAllocations + (state.storeId -> (smap - state.allocationTransactionUUID))
  }
}