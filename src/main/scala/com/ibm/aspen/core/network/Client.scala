package com.ibm.aspen.core.network

import java.util.UUID
import java.nio.ByteBuffer

/** Represents a non-store entity that can send and receive messages to and from data stores
 * 
 */
case class Client(uuid: UUID) extends NetworkID {
  
  override def equals(other: Any): Boolean = other match {
    case rhs: Client => uuid == rhs.uuid
    case _ => false
  }
}

object Client {
  
  def apply(data:Array[Byte]): Client = Client(NetworkID.decodeUUID(data))
  
}