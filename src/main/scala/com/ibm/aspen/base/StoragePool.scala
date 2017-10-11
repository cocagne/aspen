package com.ibm.aspen.base

import com.ibm.aspen.core.ida.IDA
import java.util.UUID
import com.ibm.aspen.core.network.StorageNodeID
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

trait StoragePool {
  val uuid: UUID
  
  def allocationTreeDefinitionPointer()(implicit ec: ExecutionContext): Future[ObjectPointer]
  
  /** The entries of this array describe which storage node is currently hosting the store with the corresponding index */
  def hostingStorageNodes: Array[StorageNodeID]
  
  def numberOfStores: Int = hostingStorageNodes.length
  
  def supportsIDA(ida: IDA): Boolean
  
  /** Throws AllocationError: UnsupportedIDA if the IDA is not supported*/
  def selectStoresForAllocation(ida: IDA): Array[Int]
  
}