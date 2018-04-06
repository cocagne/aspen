package com.ibm.aspen.base

import java.util.UUID
import scala.annotation.tailrec

class AggregateTypeRegistry(val subregistries: List[TypeRegistry]) extends TypeRegistry {
  
  def getTypeFactory[T <: TypeFactory](factoryUUID: UUID): Option[T] = {
    
    @tailrec
    def rfind(l: List[TypeRegistry]): Option[T] = if (l.isEmpty) None else {
      l.head.getTypeFactory[T](factoryUUID) match {
        case None => rfind(l.tail)
        case Some(tgt) => Some(tgt)
      }
    }
    
    rfind(subregistries)
  }
}