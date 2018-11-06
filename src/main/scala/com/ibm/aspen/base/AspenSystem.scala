package com.ibm.aspen.base

import java.util.UUID

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.AllocationRevisionGuard
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectPointer}
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.transaction.ClientTransactionDriver

import scala.concurrent.{ExecutionContext, Future}

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
        revisionGuard: AllocationRevisionGuard,
        poolUUID: UUID,
        objectSize: Option[Int],
        objectIDA: IDA,
        initialContent: DataBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer]
  
  def lowLevelAllocateKeyValueObject(
        revisionGuard: AllocationRevisionGuard,
        poolUUID: UUID,
        objectSize: Option[Int],
        objectIDA: IDA,
        initialContent: List[KeyValueOperation])(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer]

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
  def transactUntilSuccessfulWithRecovery[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    retryStrategy.retryUntilSuccessful(onCommitFailure) {
      transact(prepare)
    }
  }

  def getSystemAttribute(key: String): Option[String]
  def setSystemAttribute(key: String, value: String): Unit
  
  /** Immediately cancels all future activity scheduled for execution */
  def shutdown(): Unit
}