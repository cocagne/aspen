package com.ibm.aspen.base.impl

import java.util.UUID

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.transaction.TransactionStatus

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait TransactionStatusQuerier {

  def queryTransactionStatus(requestingStore: DataStoreID,
                             initialRetryDelay: Duration,
                             primaryObject: ObjectPointer,
                             transactionUUID: UUID): Future[Option[TransactionStatus.Value]]

  /** The future completes when either a finalized commit is seen, an abort resolution is seen, or the number of
    * "unknown transaction" responses equals or exceeds the object's consistent restore threshold. Note that if this
    * method is called before the stores hosting the target object begin processing the transaction, the promise
    * could complete. Which would falsely indicate that the transaction had been completed, finalized, and forgotten.
    * Users of this method must ensure correct operation in that case.
    */
  def queryFinalizedTransactionStatus(requestingStore: DataStoreID,
                                      initialRetryDelay: Duration,
                                      primaryObject: ObjectPointer,
                                      transactionUUID: UUID): Future[Option[TransactionStatus.Value]]
}
