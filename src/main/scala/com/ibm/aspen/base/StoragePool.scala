package com.ibm.aspen.base

import com.ibm.aspen.core.ida.IDA
import java.util.UUID
import com.ibm.aspen.core.network.StorageNodeID

trait StoragePool {
  def uuid: UUID
  
  /** The entries of this array describe which storage node is currently hosting the store with the corresponding index */
  def hostingStorageNodes: Array[StorageNodeID]
  
  def numberOfStores: Int = hostingStorageNodes.length
  
  def supportsIDA(ida: IDA): Boolean
  
  def selectStoresForAllocation(ida: IDA): Array[Int]
  
}