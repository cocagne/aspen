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

trait AspenSystem {
  
  def client: ClientID
  
  def readObject(pointer:ObjectPointer, readStrategy: Option[ReadDriver.Factory]): Future[ObjectStateAndData]
  
  def readObject(pointer:ObjectPointer): Future[ObjectStateAndData] = readObject(pointer, None)
  
  def newTransaction(): Transaction
  
  def allocateObject(
      allocInto: ObjectPointer,
      allocIntoRevision: ObjectRevision,
      poolUUID: UUID,
      objectSize: Option[Int],
      objectIDA: IDA,
      initialContent: ByteBuffer)(implicit t: Transaction, ec: ExecutionContext): Future[ObjectPointer]
  
  
  //def getStoragePool(poolUUID: UUID): Future[StoragePool]
  
}