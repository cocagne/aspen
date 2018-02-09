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

trait AspenSystem extends ObjectReader {
  
  def clientId: ClientID
  
  def newTransaction(): Transaction
  
  def newTransaction(transactionDriverStrategy: ClientTransactionDriver.Factory): Transaction = newTransaction()
  
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
      initialContent: Map[Array[Byte], Array[Byte]],
      minimum: Option[Key],
      maximum: Option[Key],
      left: Option[Array[Byte]],
      right: Option[Array[Byte]],
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer]

  def getStoragePool(poolUUID: UUID): Future[StoragePool]
  def getStoragePool(storagePoolDefinitionPointer: DataObjectPointer): Future[StoragePool]
  
  def createTaskGroup(groupUUID: UUID, taskGroupType: UUID, groupDefinitionContent: DataBuffer): Future[TaskGroup]
  def getTaskGroup(groupUUID: UUID): Future[TaskGroup]
  def createTaskGroupExecutor(groupUUID: UUID): Future[TaskGroupExecutor]
  
  // TODO: ObjectAllocater tree + save/restore
  //def getObjectAllocater(allocaterUUID: UUID): Future[ObjectAllocater]
  
  /** Immediately cancels all future activity scheduled for execution */
  def shutdown(): Unit
}