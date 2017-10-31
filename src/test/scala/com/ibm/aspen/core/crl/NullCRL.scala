package com.ibm.aspen.core.crl

import com.ibm.aspen.core.transaction.TransactionRecoveryState
import scala.concurrent.Future
import com.ibm.aspen.core.transaction.TransactionDescription
import java.nio.ByteBuffer
import com.ibm.aspen.core.data_store.DataStoreID

class NullCRL extends CrashRecoveryLog {
  def getFullTransactionRecoveryState(): Map[DataStoreID, List[TransactionRecoveryState]] = Map()
  
  def getTransactionRecoveryStateForStore(storeId: DataStoreID): List[TransactionRecoveryState] = Nil
  
  def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit] = Future.successful(())
  
  def discardTransactionState(txd: TransactionDescription): Unit = ()
}