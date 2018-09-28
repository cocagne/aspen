package com.ibm.aspen.base

import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import java.util.UUID
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.read.ReadDriver
import java.nio.ByteBuffer
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.base.task.DurableTaskType
import com.ibm.aspen.core.data_store.DataStoreID

trait AspenSystem extends ObjectReader {
  
  def clientId: ClientID
  
  val retryStrategy: RetryStrategy
  
  val typeRegistry: TypeRegistry
  
  def newTransaction(): Transaction
  
  def newTransaction(transactionDriverStrategy: ClientTransactionDriver.Factory): Transaction = newTransaction()
  
  /** Returns the retry strategy associated with the specified UUID or the default retryStrategy if not
   *  matching UUID is found
   */
  def getRetryStrategy(uuid: UUID): RetryStrategy
  
  def registerRetryStrategy(uuid: UUID, strategy: RetryStrategy): Unit
  
  def lowLevelAllocateDataObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: DataBuffer,
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer]
  
  def lowLevelAllocateKeyValueObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: List[KeyValueOperation],
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer]

  def getStoragePool(poolUUID: UUID): Future[StoragePool]
  def getStoragePool(storagePoolDefinitionPointer: KeyValueObjectPointer): Future[StoragePool]
  
  def getObjectAllocater(allocaterUUID: UUID): Future[ObjectAllocater]
  
  def getStorageHost(storeId: DataStoreID): Future[StorageHost]
  
  def createMissedUpdateHandler(
      mus: MissedUpdateStrategy,
      pointer: ObjectPointer, 
      missedStores: List[Byte])(implicit ec: ExecutionContext): MissedUpdateHandler
      
  def createMissedUpdateIterator(
      mus: MissedUpdateStrategy, 
      storeId: DataStoreID)(implicit ec: ExecutionContext): MissedUpdateIterator
  
  def transact[T](prepare: Transaction => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val tx = newTransaction()
    
    val fprep = try { prepare(tx) } catch {
      case err: Throwable => Future.failed(err)
    }
    
    val fresult = for {
      prepResult <- fprep
      _ <- tx.commit()
    } yield prepResult
    
    fresult.failed.foreach(err => tx.invalidateTransaction(err))
    
    fresult
  }
  
  def transactUntilSuccessful[T](prepare: Transaction => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    retryStrategy.retryUntilSuccessful {
      transact(prepare)
    }
  }
  def transactUntilSuccessfulWithRecovery[T](onCommitFailure: (Throwable) => Future[Unit])(prepare: Transaction => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    retryStrategy.retryUntilSuccessful(onCommitFailure) {
      transact(prepare)
    }
  }

  def getSystemAttribute(key: String): Option[String]
  def setSystemAttribute(key: String, value: String): Unit
  
  /** Immediately cancels all future activity scheduled for execution */
  def shutdown(): Unit
}