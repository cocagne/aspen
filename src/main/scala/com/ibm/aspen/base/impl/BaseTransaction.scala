package com.ibm.aspen.base.impl

import com.ibm.aspen.base.Transaction
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectRefcount
import scala.concurrent.Promise
import scala.concurrent.Future
import com.ibm.aspen.base.PostCommitTransactionModification
import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.core.transaction.ClientTransactionManager
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.base.TransactionAborted
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.data_store.DataStoreID

object BaseTransaction {
  def Factory(
      txManager: ClientTransactionManager,
      chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
      transactionDriverStrategy: Option[ClientTransactionDriver.Factory]) = new BaseTransaction(txManager, chooseDesignatedLeader, transactionDriverStrategy)
}

class BaseTransaction(
    txManager: ClientTransactionManager,
    chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
    transactionDriverStrategy: Option[ClientTransactionDriver.Factory]) extends Transaction {
  
  val uuid: UUID = UUID.randomUUID()
  private [this] val promise = Promise[Unit]
  private [this] var builder: Option[TransactionBuilder] = Some(new TransactionBuilder(chooseDesignatedLeader, txManager.clientId))
  
  def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = synchronized { builder } match {
    case Some(bldr) => bldr.append(objectPointer, requiredRevision, data)
    case None => throw PostCommitTransactionModification()
  }
  def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = synchronized { builder } match {
    case Some(bldr) => bldr.overwrite(objectPointer, requiredRevision, data)
    case None => throw PostCommitTransactionModification()
  }
  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount = synchronized { builder } match {
    case Some(bldr) => bldr.setRefcount(objectPointer, requiredRefcount, refcount)
    case None => throw PostCommitTransactionModification()
  }
  def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit = synchronized { builder } match {
    case Some(bldr) => bldr.addFinalizationAction(finalizationActionUUID, serializedContent)
    case None => throw PostCommitTransactionModification()
  }
  
  def addNotifyOnResolution(storesToNotify: Set[DataStoreID]): Unit = synchronized { builder } match {
    case Some(bldr) => bldr.addNotifyOnResolution(storesToNotify)
    case None => throw PostCommitTransactionModification()
  }
  
  def invalidateTransaction(reason: Throwable): Unit = synchronized {
    if (!promise.isCompleted)
      promise.failure(reason)
  }
  
  def result: Future[Unit] = promise.future
  
  /** Begins the transaction commit process and returns a Future to its completion. This is the same future as
   *  returned by 'result' 
   *  
   *  The future successfully completes if the transaction commits. Otherwise it will fail with a TransactionError subclass.  
   */
  def commit()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    if (!promise.isCompleted) {
      builder.foreach { bldr =>
        val (txd, encodedDataUpdates) = bldr.buildTranaction(uuid)
        builder = None
        if (txd.requirements.isEmpty)
          promise.success(())
        else {
            txManager.runTransaction(txd, encodedDataUpdates, transactionDriverStrategy) onComplete {
              case Failure(cause) =>
                // TODO Catch transaction timeout from lower layer and convert to TransactionError.TransactionTimedOut
                promise.failure(cause)
              case Success(committed) => if (committed) promise.success(()) else promise.failure(TransactionAborted(txd))
            }
        }
      }
    }
    result
  }
}