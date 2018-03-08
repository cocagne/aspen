package com.ibm.aspen.base

import java.util.UUID
import scala.annotation.tailrec

class AggregateTypeRegistry[T <: TypeFactory](val subregistries: List[TypeRegistry[T]]) extends TypeRegistry[T] {
  
  def getTypeFactory(factoryUUID: UUID): Option[T] = {
    
    @tailrec
    def rfind(l: List[TypeRegistry[T]]): Option[T] = if (l.isEmpty) None else {
      l.head.getTypeFactory(factoryUUID) match {
        case None => rfind(l.tail)
        case Some(tgt) => Some(tgt)
      }
    }
    
    rfind(subregistries)
  }
}