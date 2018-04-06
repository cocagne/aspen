package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.base.FinalizationAction
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.tieredlist.TieredKeyValueListSplitFA
import com.ibm.aspen.base.tieredlist.TieredKeyValueListJoinFA
import com.ibm.aspen.base.TypeRegistry
import com.ibm.aspen.base.TypeFactory

object BaseImplTypeRegistry {
  
  def apply(
    retryStrategy: RetryStrategy,
    system: BasicAspenSystem): BaseImplTypeRegistry = {
    
    val handlers = List(
        new AllocationFinalizationAction(retryStrategy, system),
        new TieredKeyValueListSplitFA(retryStrategy, system),
        new TieredKeyValueListJoinFA(retryStrategy, system)
    )
    
    new BaseImplTypeRegistry(handlers)
  }
}

class BaseImplTypeRegistry private (handlers: List[TypeFactory]) extends TypeRegistry {
  
  val handlerMap = handlers.map(h => (h.typeUUID -> h)).toMap
  
  override def getTypeFactory[T <: TypeFactory](typeUUID: UUID): Option[T] = handlerMap.get(typeUUID).map(tf => tf.asInstanceOf[T])
}