package com.ibm.aspen.core.transaction

import java.util.UUID

final case class SerializedFinalizationAction(
    /** Identifies the type of the serialized FinalizationAction
     *  
     *  UUIDs are used instead of an enumeration in order to support the definition of arbitrary,
     *  application-level FinalizationActions
     */
    typeUUID: UUID,
    
    /** Serialized FinalizationAction */
    data: Array[Byte]) {
  override def equals(other: Any): Boolean = other match {
    case rhs: SerializedFinalizationAction => typeUUID == rhs.typeUUID && java.util.Arrays.equals(data, rhs.data)
    case _ => false
  }
}