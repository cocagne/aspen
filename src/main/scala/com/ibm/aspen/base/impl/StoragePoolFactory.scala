package com.ibm.aspen.base.impl

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.ObjectPointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.StoragePool
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer

trait StoragePoolFactory {
  def createStoragePool(
      system: AspenSystem, 
      poolDefinitionPointer: KeyValueObjectPointer)(implicit ec: ExecutionContext): Future[StoragePool]
}