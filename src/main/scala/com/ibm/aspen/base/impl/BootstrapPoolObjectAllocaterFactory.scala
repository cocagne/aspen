package com.ibm.aspen.base.impl

import com.ibm.aspen.base.ObjectAllocaterFactory
import com.ibm.aspen.base.AspenSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.core.ida.IDA

class BootstrapPoolObjectAllocaterFactory(val bootstrapPoolIDA: IDA) extends ObjectAllocaterFactory {
  
  val typeUUID = Bootstrap.BootstrapObjectAllocaterUUID
  
  def create(system: AspenSystem)(implicit ec: ExecutionContext): Future[ObjectAllocater] = {
    Future.successful(new SinglePoolObjectAllocater(system, 
      Bootstrap.BootstrapObjectAllocaterUUID, Bootstrap.BootstrapStoragePoolUUID, None, bootstrapPoolIDA))
  }
}