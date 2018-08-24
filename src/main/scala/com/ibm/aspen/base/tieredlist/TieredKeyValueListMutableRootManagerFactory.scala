package com.ibm.aspen.base.tieredlist

import com.ibm.aspen.base.TypeFactory
import com.ibm.aspen.core.DataBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.AspenSystem

trait TieredKeyValueListMutableRootManagerFactory extends TypeFactory {
  def createMutableRootManager(
      system: AspenSystem, 
      serializedRootManager: DataBuffer)(implicit ec: ExecutionContext): Future[TieredKeyValueListMutableRootManager]
}