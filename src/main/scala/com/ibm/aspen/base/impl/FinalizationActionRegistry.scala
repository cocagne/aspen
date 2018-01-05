package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.base.FinalizationAction
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.kvtree.KVTreeFactory
import com.ibm.aspen.base.kvtree.KVTreeFinalizationActionHandler
import com.ibm.aspen.base.impl.task.TaskCreationFinalizationAction

object FinalizationActionRegistry {
  def apply(
    retryStrategy: RetryStrategy,
    system: BasicAspenSystem,
    kvTreeFactory: KVTreeFactory): FinalizationActionRegistry = {
    
    val handlers = List(
        new KVTreeFinalizationActionHandler(kvTreeFactory, retryStrategy, system),
        new AllocationFinalizationAction(retryStrategy, system),
        new TaskCreationFinalizationAction(retryStrategy, system)
    )
    
    new FinalizationActionRegistry(handlers)
  }
}

class FinalizationActionRegistry private (handlers: List[FinalizationActionHandler]) extends FinalizationActionHandler {
  
  val supportedUUIDs: Set[UUID] = handlers.foldLeft(Set[UUID]())( (s, h) => s ++ h.supportedUUIDs )  
  
  private val actionMap: Map[UUID, FinalizationActionHandler] = handlers.foldLeft(Map[UUID, FinalizationActionHandler]()) {
    (m, h) => h.supportedUUIDs.foldLeft(m)( (m2, uuid) => m2 + (uuid -> h))
  }
  
  def createAction(
      finalizationActionUUID: UUID, 
      serializedActionData: Array[Byte]): Option[FinalizationAction] = {
    actionMap.get(finalizationActionUUID) match {
      case None => None
      case Some(handler) => handler.createAction(finalizationActionUUID, serializedActionData)
    }
  }
}