package com.ibm.aspen.base

import com.ibm.aspen.core.ida.IDA
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.KeyValueObjectState


trait StoragePool {
  val uuid: UUID
  
  val poolDefinitionPointer: KeyValueObjectPointer
  
  def getAllocationTree(retryStrategy: RetryStrategy)(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList]
  
  /** The entries of this array describe which storage host that currently owns store with the corresponding index */
  def storageHosts: Array[StorageHost]
  
  def numberOfStores: Int = storageHosts.length
  
  def supportsIDA(ida: IDA): Boolean
  
  /** Throws AllocationError: UnsupportedIDA if the IDA is not supported*/
  def selectStoresForAllocation(ida: IDA): Array[Int]
  
  def getMissedUpdateStrategy(): MissedUpdateStrategy
  
  def createMissedUpdateHandler(
      pointer: ObjectPointer, 
      missedStores: List[Byte])(implicit ec: ExecutionContext): MissedUpdateHandler
      
  def createMissedUpdateIterator(poolIndex: Byte)(implicit ec: ExecutionContext): MissedUpdateIterator
  
  def refresh()(implicit ec: ExecutionContext): Future[StoragePool]
}