package com.ibm.aspen.core.network

import java.util.UUID
import java.nio.ByteBuffer

abstract class NetworkID {
  val uuid: UUID
  
  def serialized: Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(0, uuid.getMostSignificantBits)
    bb.putLong(8, uuid.getLeastSignificantBits)
    bb.array()
  }
  
}

object NetworkID {
  def decodeUUID(data:Array[Byte]): UUID = {
    assert(data.length == 16, "Serialized UUID is not of length 16!")
    
    val bb = ByteBuffer.wrap(data)
    
    new UUID(bb.getLong(0), bb.getLong(8))
  }
}