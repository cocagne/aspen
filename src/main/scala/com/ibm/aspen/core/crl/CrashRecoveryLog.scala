package com.ibm.aspen.core.crl

import com.ibm.aspen.core.transaction.TransactionRecoveryState
import scala.concurrent.Future
import com.ibm.aspen.core.transaction.TransactionDescription
import java.nio.ByteBuffer
import com.ibm.aspen.core.data_store.DataStoreID

trait CrashRecoveryLog {
  
  def getFullTransactionRecoveryState(): Map[DataStoreID, List[TransactionRecoveryState]]
  
  def getTransactionRecoveryStateForStore(storeId: DataStoreID): List[TransactionRecoveryState]
  
  /** Returns a Future to successfully saving the transaction state.
   *
   * Note: Failure will be returned if the recovery state cannot be saved. This can happen if
   *       if the media hosting the state fails or the save method is called while the CRL is being shut down
   */
  def saveTransactionRecoveryState(state: TransactionRecoveryState): Future[Unit]
  
  def discardTransactionState(txd: TransactionDescription): Unit
}