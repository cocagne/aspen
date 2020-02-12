package com.ibm.amoeba.client

import com.ibm.amoeba.common.objects.ObjectId
import com.ibm.amoeba.common.store.StoreId

import scala.concurrent.Future

trait FinalizationAction {

  def complete: Future[Unit]

  def execute(): Unit

  /** Called each time an TxCommitted message is received from a new peer. The provided map contains the
    * aggregated content from all previously-received TxCommitted messages. The list of UUIDs contains the
    * object UUIDs that could not be updated as part of the transaction due to transaction requirement
    * mismatches. Mostly likely, the object copy/slices on those stores will need to be repaired.
    *
    * Finalizers are created immediately upon reaching the threshold commit requirement. Finalizers
    * that depend on knowing which peers have successfully processed the transaction (as indicated by
    * them sending Accepted messages) may delay action for some time to receive additional notifications
    * via this callback.
    */
  def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = ()

  def debugStatus: (String, Boolean) = synchronized {
    (this.getClass.toString, complete.isCompleted)
  }

  /** Called when a TxFinalized message is received. */
  def cancel(): Unit = ()
}
