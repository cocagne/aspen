package com.ibm.aspen.base.impl

import java.util.UUID

import com.ibm.aspen.base.{PostCommitTransactionModification, Transaction, TransactionAborted}
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects._
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.transaction.{ClientTransactionDriver, ClientTransactionManager, KeyValueUpdate}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object BaseTransaction {
  def Factory(
      system: BasicAspenSystem,
      txManager: ClientTransactionManager,
      chooseDesignatedLeader: ObjectPointer => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
      transactionDriverStrategy: Option[ClientTransactionDriver.Factory]) = new BaseTransaction(system, txManager, chooseDesignatedLeader, transactionDriverStrategy)
}

class BaseTransaction(
    val system: BasicAspenSystem,
    txManager: ClientTransactionManager,
    chooseDesignatedLeader: ObjectPointer => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
    transactionDriverStrategy: Option[ClientTransactionDriver.Factory]) extends Transaction with Logging {
  
  val uuid: UUID = UUID.randomUUID()
  private [this] val promise = Promise[HLCTimestamp]
  private [this] var state: Either[HLCTimestamp, TransactionBuilder] = Right(new TransactionBuilder(uuid, chooseDesignatedLeader, txManager.clientId))
  private [this] var invalidated = false
  private [this] var havePendingUpdates = false

  private [this] val stack = com.ibm.aspen.util.getStack() // for debugging
  
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

  def note(note: String): Unit = synchronized { state } match {
    case Right(bldr) => bldr.note(note)
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
        val (txd, encodedDataUpdates, timestamp) = bldr.buildTranaction(system.opportunisticRebuildManager, uuid)

        //---- Tx Debugging ----
        result.onComplete {
          case Success(_) =>
            logger.info(s"TX SUCCESS: ${txd.shortString}\n${txd.shortString}")
          case Failure(e) =>
            logger.info(s"TX FAILURE: ${txd.shortString}: $e\n$stack")
        }
        //---------------------

        state = Left(timestamp)
        if (txd.requirements.isEmpty)
          promise.success(HLCTimestamp(txd.startTimestamp))
        else {
          txManager.runTransaction(txd, encodedDataUpdates, transactionDriverStrategy) onComplete {
            case Failure(cause) =>
              // TODO Catch transaction timeout from lower layer and convert to TransactionError.TransactionTimedOut
              system.transactionCache.transactionAborted(txd.transactionUUID)
              promise.failure(cause)
            case Success(committed) =>
              if (committed) {
                val ts = HLCTimestamp(txd.startTimestamp)
                HLCTimestamp.update(ts)
                system.transactionCache.transactionCommitted(txd.transactionUUID)
                promise.success(ts)
              } else 
                promise.failure(TransactionAborted(txd))
              
          }
        }
      }
    }
    result
  }
}