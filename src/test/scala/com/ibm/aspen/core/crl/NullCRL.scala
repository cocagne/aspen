package com.ibm.aspen.core.crl

import com.ibm.aspen.core.transaction.TransactionRecoveryState
import scala.concurrent.Future
import com.ibm.aspen.core.transaction.TransactionDescription
import java.nio.ByteBuffer
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import java.util.UUID

class NullCRL extends CrashRecoveryLog {
  def getFullTransactionRecoveryState(): Map[DataStoreID, List[TransactionRecoveryState]] = Map()
  
  def getTransactionRecoveryStateForStore(storeId: DataStoreID): List[TransactionRecoveryState] = Nil
  
  def getFullAllocationRecoveryState(): Map[DataStoreID, List[AllocationRecoveryState]] = Map()
  
  def getAllocationRecoveryStateForStore(storeId: DataStoreID): List[AllocationRecoveryState] = Nil
  
  def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = Future.successful(())
  
  def discardTransactionState(storeId: DataStoreID, txd: TransactionDescription): Unit = ()
  
  def saveAllocationRecoveryState(state: AllocationRecoveryState): Future[Unit] = Future.successful(())
  
  def discardAllocationState(storeId: DataStoreID, allocationTransactionUUID: UUID): Unit = ()
  
  def close(): Future[Unit] = Future.successful(())
}