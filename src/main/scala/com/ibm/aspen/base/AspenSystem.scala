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

trait AspenSystem {
  
  def clientId: ClientID
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:ObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[ObjectStateAndData]
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:ObjectPointer): Future[ObjectStateAndData] = readObject(pointer, None)
  
  def newTransaction(): Transaction
  
  def newTransaction(transactionDriverStrategy: ClientTransactionDriver.Factory): Transaction = newTransaction()
  
  def allocateObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: ByteBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer]
  
  def allocateObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: Array[Byte])(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer] = {
    allocateObject(allocatingObject, allocatingObjectRevision, poolUUID, objectSize, objectIDA, ByteBuffer.wrap(initialContent))
  }
  
  def getStoragePool(poolUUID: UUID): Future[StoragePool]
  def getStoragePool(storagePoolDefinitionPointer: ObjectPointer): Future[StoragePool] 
  
}