package com.ibm.aspen.core.network

import java.util.UUID

/** Represents a storage node where storage nodes host one or more data stores
 * 
 */
case class StorageNodeID(uuid: UUID) extends NetworkID {
  
  override def equals(other: Any): Boolean = other match {
    case rhs: StorageNodeID => uuid == rhs.uuid
    case _ => false
  }
}

object StorageNodeID {
  
  def apply(data:Array[Byte]): StorageNodeID = StorageNodeID(NetworkID.decodeUUID(data))
  
}