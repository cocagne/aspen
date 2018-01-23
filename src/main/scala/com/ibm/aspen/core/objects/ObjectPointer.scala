package com.ibm.aspen.core.objects

import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.data_store.DataStoreID

sealed abstract class ObjectPointer(
    val uuid: UUID,
    val poolUUID: UUID,
    val size: Option[Int],
    val ida: IDA,
    val storePointers: Array[StorePointer]) {
  
  final override def equals(other: Any): Boolean = other match {
    case rhs: ObjectPointer => uuid == rhs.uuid && poolUUID == rhs.poolUUID && size == rhs.size &&
     ida == rhs.ida && java.util.Arrays.equals(storePointers.asInstanceOf[Array[Object]], rhs.storePointers.asInstanceOf[Array[Object]])
    case _ => false
  }
  
  final override def hashCode: Int = uuid.hashCode()
  
  def getStorePointer(storeId: DataStoreID): Option[StorePointer] = if (storeId.poolUUID == poolUUID) {
    storePointers.find(sp => sp.poolIndex == storeId.poolIndex)
  } else
    None
    
  def objectType: ObjectType.Value
    
  protected def addExtraToStringContent(sb: StringBuilder): Unit = {}
    
  override def toString(): String = {
    val sb = new StringBuilder

    sb.append(objectType.toString)
    sb.append("ObjectPointer(")
    sb.append(uuid.toString)
    sb.append(',')
    sb.append(poolUUID.toString)
    sb.append(',')
    sb.append(size.toString)
    sb.append(',')
    sb.append(ida.toString)
    sb.append(',')
    addExtraToStringContent(sb)
    sb.append('[')
    storePointers.foreach { sp =>
      sb.append(sp.toString)
      sb.append(',')
    }
    sb.append(']')
    sb.toString()
  }
}

class DataObjectPointer(
    uuid: UUID,
    poolUUID: UUID,
    size: Option[Int],
    ida: IDA,
    storePointers: Array[StorePointer]) extends ObjectPointer(uuid, poolUUID, size, ida, storePointers) {
  
  override def objectType: ObjectType.Value = ObjectType.Data
}

object DataObjectPointer {
  def apply(
      uuid: UUID,
      poolUUID: UUID,
      size: Option[Int],
      ida: IDA,
      storePointers: Array[StorePointer]): DataObjectPointer = new DataObjectPointer(uuid, poolUUID, size, ida, storePointers)
}

class KeyValueObjectPointer(
    uuid: UUID,
    poolUUID: UUID,
    size: Option[Int],
    ida: IDA,
    storePointers: Array[StorePointer]) extends ObjectPointer(uuid, poolUUID, size, ida, storePointers) {
  
  override def objectType: ObjectType.Value = ObjectType.KeyValue
}

object KeyValueObjectPointer {
  def apply(
      uuid: UUID,
      poolUUID: UUID,
      size: Option[Int],
      ida: IDA,
      storePointers: Array[StorePointer]): KeyValueObjectPointer = new KeyValueObjectPointer(uuid, poolUUID, size, ida, storePointers)
  
}