package com.ibm.aspen.base.impl

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.network.StorageNodeID
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.StoragePool
import com.ibm.aspen.core.objects.DataObjectPointer

trait StoragePoolFactory {
  def createStoragePool(
      system: AspenSystem, 
      poolDefinitionPointer: DataObjectPointer, 
      isStorageNodeOnline: (StorageNodeID) => Boolean)(implicit ec: ExecutionContext): Future[StoragePool]
}