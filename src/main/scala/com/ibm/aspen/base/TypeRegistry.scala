package com.ibm.aspen.base

import java.util.UUID

/** Serves as a mapping between UUIDs and the user-extensible types that plug into the Aspen system.
 * 
 */
trait TypeRegistry[T <: TypeFactory] {
  def getTypeFactory(typeUUID: UUID): Option[T]
}