package com.ibm.aspen.base

import java.util.UUID

/** Serves as a mapping between UUIDs and the user-extensible types that plug into the Aspen system.
 * 
 */
trait TypeRegistry {
  protected[base] def getTypeFactory[T <: TypeFactory](typeUUID: UUID): Option[T]
  
  def lookup[T <: TypeFactory](typeUUID: UUID): Option[T] = getTypeFactory(typeUUID) match {
    case None => None
    case Some(t) => 
      try {
        Some(t.asInstanceOf[T])
      } catch {
        case e: ClassCastException => None
      }
  }
}