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
import com.ibm.aspen.base.task.TaskGroupRegistry
import com.ibm.aspen.base.AggregateTypeRegistry
import com.ibm.aspen.base.tieredlist.SimpleTieredKeyValueListNodeAllocater
import com.ibm.aspen.base.tieredlist.MutableKeyValueObjectRootManager
import com.ibm.aspen.base.tieredlist.MutableTKVLRootManager

object BaseImplTypeRegistry {
  
  def apply(system: BasicAspenSystem): TypeRegistry = {
    
    val subRegistries: List[TypeRegistry] = List(
      TaskGroupRegistry
    )
    
    val handlers = List(
        new AllocationFinalizationAction,
        new DeleteFinalizationAction,
        new TieredKeyValueListSplitFA,
        new TieredKeyValueListJoinFA,
        PerStoreMissedUpdate,
        MissedUpdateFinalizationAction,
        
        SimpleTieredKeyValueListNodeAllocater,
        MutableKeyValueObjectRootManager,
        MutableTKVLRootManager,
        
        new BootstrapPoolObjectAllocaterFactory(system.bootstrapPoolIDA)
    )
    
    class DirectRegistry extends TypeRegistry {
  
      val handlerMap = handlers.map(h => (h.typeUUID -> h)).toMap
      
      override def getTypeFactory[T <: TypeFactory](typeUUID: UUID): Option[T] = handlerMap.get(typeUUID).map(tf => tf.asInstanceOf[T])
    }
    
    new AggregateTypeRegistry(new DirectRegistry :: subRegistries)
  }
}

