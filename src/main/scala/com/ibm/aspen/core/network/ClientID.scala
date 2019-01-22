package com.ibm.aspen.core.network

import java.util.UUID
import java.nio.ByteBuffer

/** Represents a non-store entity that can send and receive messages to and from data stores
 * 
 */
case class ClientID(uuid: UUID) extends NetworkID {
  
  override def equals(other: Any): Boolean = other match {
    case rhs: ClientID => uuid == rhs.uuid
    case _ => false
  }
}

object ClientID {
  
  def apply(data:Array[Byte]): ClientID = ClientID(NetworkID.decodeUUID(data))

  val Null = ClientID(new UUID(0,0))
}