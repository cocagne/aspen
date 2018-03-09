package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.base.FinalizationAction
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.tieredlist.TieredKeyValueListSplitFA
import com.ibm.aspen.base.tieredlist.TieredKeyValueListJoinFA
import com.ibm.aspen.base.TypeRegistry

object BaseFinalizationActionHandlerRegistry {
  def apply(
    retryStrategy: RetryStrategy,
    system: BasicAspenSystem): BaseFinalizationActionHandlerRegistry = {
    
    val handlers = List(
        new AllocationFinalizationAction(retryStrategy, system),
        new TieredKeyValueListSplitFA(retryStrategy, system),
        new TieredKeyValueListJoinFA(retryStrategy, system)
    )
    
    new BaseFinalizationActionHandlerRegistry(handlers)
  }
}

class BaseFinalizationActionHandlerRegistry private (handlers: List[FinalizationActionHandler]) extends TypeRegistry[FinalizationActionHandler] {
  
  val handlerMap = handlers.map(h => (h.typeUUID -> h)).toMap
  
  def getTypeFactory(typeUUID: UUID): Option[FinalizationActionHandler] = handlerMap.get(typeUUID)
}