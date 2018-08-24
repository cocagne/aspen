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
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.transaction.KeyValueUpdate

object BaseTransaction {
  def Factory(
      system: BasicAspenSystem,
      txManager: ClientTransactionManager,
      chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
      transactionDriverStrategy: Option[ClientTransactionDriver.Factory]) = new BaseTransaction(system, txManager, chooseDesignatedLeader, transactionDriverStrategy)
}

class BaseTransaction(
    val system: BasicAspenSystem,
    txManager: ClientTransactionManager,
    chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
    transactionDriverStrategy: Option[ClientTransactionDriver.Factory]) extends Transaction {
  
  val uuid: UUID = UUID.randomUUID()
  private [this] val promise = Promise[HLCTimestamp]
  private [this] var state: Either[HLCTimestamp, TransactionBuilder] = Right(new TransactionBuilder(uuid, chooseDesignatedLeader, txManager.clientId))
  private [this] var invalidated = false
  private [this] var havePendingUpdates = false
  
  def valid: Boolean = synchronized { !invalidated && havePendingUpdates }
  
  def disableMissedUpdateTracking(): Unit = synchronized { state } match {
    case Right(bldr) => bldr.disableMissedUpdateTracking()
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def setMissedCommitDelayInMs(msec: Int): Unit = synchronized { state } match {
    case Right(bldr) => bldr.setMissedCommitDelayInMs(msec)
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def append(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = synchronized { state } match {
    case Right(bldr) =>
      havePendingUpdates = true
      bldr.append(objectPointer, requiredRevision, data)
    case Left(_) => throw PostCommitTransactionModification()
  }
  def overwrite(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = synchronized { state } match {
    case Right(bldr) =>
      havePendingUpdates = true
      bldr.overwrite(objectPointer, requiredRevision, data)
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def update(
      pointer: KeyValueObjectPointer, 
      requiredRevision: Option[ObjectRevision],
      requirements: List[KeyValueUpdate.KVRequirement],
      operations: List[KeyValueOperation]): Unit = synchronized { state } match {
    case Right(bldr) =>
      havePendingUpdates = true
      bldr.update(pointer, requiredRevision, requirements, operations)
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount = synchronized { state } match {
    case Right(bldr) =>
      havePendingUpdates = true
      bldr.setRefcount(objectPointer, requiredRefcount, refcount)
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): ObjectRevision = synchronized { state } match {
    case Right(bldr) =>
      havePendingUpdates = true
      bldr.bumpVersion(objectPointer, requiredRevision)
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def lockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit = synchronized { state } match {
    case Right(bldr) => bldr.lockRevision(objectPointer, requiredRevision)
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def ensureHappensAfter(timestamp: HLCTimestamp): Unit = synchronized { state } match {
    case Right(bldr) => bldr.ensureHappensAfter(timestamp)
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit = synchronized { state } match {
    case Right(bldr) => bldr.addFinalizationAction(finalizationActionUUID, serializedContent)
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def addFinalizationAction(finalizationActionUUID: UUID): Unit = synchronized { state } match {
    case Right(bldr) => bldr.addFinalizationAction(finalizationActionUUID)
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def addNotifyOnResolution(storesToNotify: Set[DataStoreID]): Unit = synchronized { state } match {
    case Right(bldr) => bldr.addNotifyOnResolution(storesToNotify)
    case Left(_) => throw PostCommitTransactionModification()
  }
  
  def invalidateTransaction(reason: Throwable): Unit = synchronized {
    invalidated = true
    if (!promise.isCompleted)
      promise.failure(reason)
  }
  
  def result: Future[HLCTimestamp] = promise.future
  
  /** Begins the transaction commit process and returns a Future to its completion. This is the same future as
   *  returned by 'result' 
   *  
   *  The future successfully completes if the transaction commits. Otherwise it will fail with a TransactionError subclass.  
   */
  def commit()(implicit ec: ExecutionContext): Future[HLCTimestamp] = synchronized {
    if (!promise.isCompleted) {
      state.foreach { bldr =>
        val (txd, encodedDataUpdates, timestamp) = bldr.buildTranaction(uuid)
        state = Left(timestamp)
        if (txd.requirements.isEmpty)
          promise.success(HLCTimestamp(txd.startTimestamp))
        else {
          txManager.runTransaction(txd, encodedDataUpdates, transactionDriverStrategy) onComplete {
            case Failure(cause) =>
              // TODO Catch transaction timeout from lower layer and convert to TransactionError.TransactionTimedOut
              promise.failure(cause)
            case Success(committed) =>
              if (committed) {
                system.transactionCache.put(txd.transactionUUID, true)
                promise.success(HLCTimestamp(txd.startTimestamp)) 
              } else 
                promise.failure(TransactionAborted(txd))
              
          }
        }
      }
    }
    result
  }
}