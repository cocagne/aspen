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

trait AspenSystem {
  
  def clientId: ClientID
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:DataObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[ObjectStateAndData]
  
  /** Reads and returns the current state of the object. No caches are used */
  def readObject(pointer:DataObjectPointer): Future[ObjectStateAndData] = readObject(pointer, None)
  
  def newTransaction(): Transaction
  
  def newTransaction(transactionDriverStrategy: ClientTransactionDriver.Factory): Transaction = newTransaction()
  
  def lowLevelAllocateObject(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      poolUUID: UUID,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: DataBuffer,
      afterTimestamp: Option[HLCTimestamp] = None)(implicit t: Transaction, ec: ExecutionContext): Future[DataObjectPointer]

  def getStoragePool(poolUUID: UUID): Future[StoragePool]
  def getStoragePool(storagePoolDefinitionPointer: DataObjectPointer): Future[StoragePool]
  
  def createTaskGroup(groupUUID: UUID, taskGroupType: UUID, groupDefinitionContent: DataBuffer): Future[TaskGroup]
  def getTaskGroup(groupUUID: UUID): Future[TaskGroup]
  def createTaskGroupExecutor(groupUUID: UUID): Future[TaskGroupExecutor]
  
  // TODO: ObjectAllocater tree + save/restore
  //def getObjectAllocater(allocaterUUID: UUID): Future[ObjectAllocater]
  
}