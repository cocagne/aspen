package com.ibm.aspen.base

import com.ibm.aspen.core.data_store.DataStoreID
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectPointer

trait MissedUpdateHandlerFactory extends TypeFactory {
  def createHandler(
      mus: MissedUpdateStrategy,
      system: AspenSystem,
      pointer: ObjectPointer, 
      missedStores: List[Byte])(implicit ec: ExecutionContext): MissedUpdateHandler
      
  def createIterator(
      mus: MissedUpdateStrategy,
      system: AspenSystem,
      storeId: DataStoreID)(implicit ec: ExecutionContext): MissedUpdateIterator
}