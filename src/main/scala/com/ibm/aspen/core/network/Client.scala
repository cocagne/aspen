package com.ibm.aspen.core.network

import java.util.UUID
import java.nio.ByteBuffer

/** Represents a non-store entity that can send and receive messages to and from data stores
 * 
 */
case class Client(uuid: UUID) {
  
  def serialized: Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(0, uuid.getMostSignificantBits)
    bb.putLong(8, uuid.getLeastSignificantBits)
    bb.array()
  }
  
  override def equals(other: Any): Boolean = other match {
    case rhs: Client => uuid == rhs.uuid
    case _ => false
  }
}

object Client {
  
  def apply(data:Array[Byte]): Client = {
    assert(data.length == 16, "Serialized Client UUID is not of length 16!")
    
    val bb = ByteBuffer.wrap(data)
    
    Client(new UUID(bb.getLong(0), bb.getLong(8)))
  }
}