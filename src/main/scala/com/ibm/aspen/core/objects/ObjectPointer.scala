package com.ibm.aspen.core.objects

import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.data_store.DataStoreID

final case class ObjectPointer(
    uuid: UUID,
    poolUUID: UUID,
    size: Option[Int],
    ida: IDA,
    storePointers: Array[StorePointer]) {
  
  override def equals(other: Any): Boolean = other match {
    case rhs: ObjectPointer => uuid == rhs.uuid && poolUUID == rhs.poolUUID && size == rhs.size &&
     ida == rhs.ida && java.util.Arrays.equals(storePointers.asInstanceOf[Array[Object]], rhs.storePointers.asInstanceOf[Array[Object]])
    case _ => false
  }
  
  override def hashCode: Int = uuid.hashCode()
  
  def uuidAsByteArray: Array[Byte] = com.ibm.aspen.core.Util.uuid2byte(uuid)
  
  def getStorePointer(storeId: DataStoreID): Option[StorePointer] = if (storeId.poolUUID == poolUUID) {
    storePointers.find(sp => sp.poolIndex == storeId.poolIndex)
  } else
    None
    
  override def toString(): String = {
    val sb = new StringBuilder
    sb.append("ObjectPointer(")
    sb.append(uuid.toString)
    sb.append(',')
    sb.append(poolUUID.toString)
    sb.append(',')
    sb.append(size.toString)
    sb.append(',')
    sb.append(ida.toString)
    sb.append(',')
    sb.append('[')
    storePointers.foreach { sp =>
      sb.append(sp.toString)
      sb.append(',')
    }
    sb.append(']')
    sb.toString()
  }
}