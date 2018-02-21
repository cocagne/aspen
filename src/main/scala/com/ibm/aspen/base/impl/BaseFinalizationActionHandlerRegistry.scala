package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.base.FinalizationAction
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.kvtree.KVTreeFactory
import com.ibm.aspen.base.kvtree.KVTreeFinalizationActionHandler
import com.ibm.aspen.base.impl.task.TaskCreationFinalizationAction
import com.ibm.aspen.base.FinalizationActionHandlerRegistry
import com.ibm.aspen.base.tieredlist.TieredKeyValueListSplitFA

object BaseFinalizationActionHandlerRegistry {
  def apply(
    retryStrategy: RetryStrategy,
    system: BasicAspenSystem,
    kvTreeFactory: KVTreeFactory): BaseFinalizationActionHandlerRegistry = {
    
    val handlers = List(
        new KVTreeFinalizationActionHandler(kvTreeFactory, retryStrategy, system),
        new AllocationFinalizationAction(retryStrategy, system),
        new TaskCreationFinalizationAction(retryStrategy, system),
        new TieredKeyValueListSplitFA(retryStrategy, system)
    )
    
    new BaseFinalizationActionHandlerRegistry(handlers)
  }
}

class BaseFinalizationActionHandlerRegistry private (handlers: List[FinalizationActionHandler]) extends FinalizationActionHandlerRegistry {
  
  val handlerMap = handlers.map(h => (h.finalizationActionUUID -> h)).toMap
  
  def getFinalizationActionHandler(finalizationActionUUID: UUID): Option[FinalizationActionHandler] = handlerMap.get(finalizationActionUUID)
}