package com.ibm.aspen.core.data_store

import java.util.UUID
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.concurrent.Future
import com.ibm.aspen.core.transaction.LocalUpdateContent

trait DataStore {
  
  /** Defines the Storage Pool this store belongs to and the Index of this store within the pool */
  def storeId: DataStoreID
  
  /** Returns a future to a map of the current object state for all hosted objects referenced by the TransactionDescription
   *  
   *  This method always returns a Success(). Any errors encountered along the way are noted within the CurrentObjectState
   *  associated with the object(s) for which errors were encountered. 
   */
  def getCurrentObjectState(txd: TransactionDescription): Future[ Map[UUID, Either[ObjectError.Value, CurrentObjectState]] ] 
  
  /** Locks all objects referenced by the transaction or returns a map of collisions */
  def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, TransactionDescription]]
  
  
  /** Commits the transaction changes and returns a Future to the completion of the commit operation.
   *  
   *  This method always returns Success() since there are no recovery steps the transaction logic can take for failures
   *  that occur after the commit decision has been made. 
   */
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: LocalUpdateContent): Future[Unit]
}