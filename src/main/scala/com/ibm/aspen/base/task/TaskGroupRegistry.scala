package com.ibm.aspen.base.task

import com.ibm.aspen.base.TypeRegistry
import com.ibm.aspen.base.TypeFactory
import java.util.UUID

object TaskGroupRegistry extends TypeRegistry {
  
  def getTypeFactory[T <: TypeFactory](typeUUID: UUID): Option[T] = typeUUID match {
    case LocalTaskGroup.typeUUID => Some(LocalTaskGroup.asInstanceOf[T])
    case _ => None
  }
}