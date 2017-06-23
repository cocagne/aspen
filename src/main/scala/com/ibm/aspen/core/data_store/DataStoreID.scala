package com.ibm.aspen.core.data_store

import java.util.UUID

case class DataStoreID(poolUUID: UUID, poolIndex: Byte) {
  
  override def equals(other: Any): Boolean = other match {
    case rhs: DataStoreID => poolUUID == rhs.poolUUID && poolIndex == rhs.poolIndex
    case _ => false
  }
  
  override def hashCode: Int = poolUUID.hashCode() + poolIndex
}