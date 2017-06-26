package com.ibm.aspen.core.network

/** Represents a non-store entity that can send and receive messages to and from data stores
 * 
 */
trait Client {
  def serialized: Array[Byte]
}

object Client {
  case class SimpleClient(bytes: Array[Byte]) extends Client {
    def serialized: Array[Byte] = bytes
    
    override def equals(other: Any): Boolean = other match {
      case rhs: SimpleClient => java.util.Arrays.equals(bytes, rhs.bytes)
      case _ => false
    }
  }
  def fromSerialized(data:Array[Byte]): Client = SimpleClient(data)
}