package com.ibm.aspen.core.objects

import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.util.Varint
import java.nio.ByteBuffer

sealed abstract class ObjectPointer(
    val uuid: UUID,
    val poolUUID: UUID,
    val size: Option[Int],
    val ida: IDA,
    val storePointers: Array[StorePointer]) {
  
  import ObjectPointer._
  
  def toArray: Array[Byte] = encodeToByteArray(this)
  
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
  
object ObjectPointer {
  protected val DataObjectPointerCode: Byte = 0
  protected val KeyValueObjectPointerCode: Byte = 1
  
  val EmptyArray = new Array[Byte](0)
  
  def bytesNeededForBits(numBits: Int): Int = if (numBits <= 8) 1 else {
    if ( numBits % 8 == 0 ) 
      numBits / 8 
    else 
      (numBits / 8) + 1
  }
  
  def fromArray(arr: Array[Byte]): ObjectPointer = {
    val bb = ByteBuffer.wrap(arr)
    val typeCode = bb.get()
    
    def getUUID(): UUID = {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      new UUID(msb,lsb)
    }
    val objectUUID = getUUID()
    val poolUUID = getUUID()
    val rawSize = Varint.getUnsignedInt(bb)
    val ida = IDA.deserializeIDAType(bb)
    val indexMaskLen = bb.get()
    val indexMask = new Array[Byte](indexMaskLen)
    bb.get(indexMask)
    
    val spList = Range(0, indexMaskLen*8).foldLeft(List[StorePointer]()) { (l, idx) =>
      val byte = idx / 8
      val bit = idx % 8
      val thisStore = indexMask(byte) & (1 << bit).asInstanceOf[Byte]
      
      if (thisStore != 0) {
        val spArr = if (bb.remaining() > 0) {
          val len = Varint.getUnsignedInt(bb)
          val spArr = new Array[Byte](len)
          bb.get(spArr)
          spArr
        } else
          EmptyArray
          
        StorePointer(idx.asInstanceOf[Byte], spArr) :: l
      } else 
        l
    }
    
    val spArray = spList.reverse.toArray
    
    val size = if (rawSize == 0) None else Some(rawSize)
    
    typeCode match {
      case DataObjectPointerCode => new DataObjectPointer(objectUUID, poolUUID, size, ida, spArray)
      case KeyValueObjectPointerCode => KeyValueObjectPointer(objectUUID, poolUUID, size, ida, spArray)
    }
  }
  
  def encodeToByteArray(o: ObjectPointer): Array[Byte] = {
    
    val sorted = o.storePointers.sortBy(sp => sp.poolIndex)
    
    val sizeLen = Varint.getUnignedIntEncodingLength(o.size.getOrElse(0))
    
    val idaLen = o.ida.getSerializedIDATypeLength()
    
    val indexMaskLen = bytesNeededForBits(sorted(sorted.length-1).poolIndex)
    
    val indexMask = new Array[Byte](indexMaskLen)
    
    sorted.foreach { sp =>
      val byte = sp.poolIndex / 8
      val bit = sp.poolIndex % 8
      indexMask(byte) = (indexMask(byte) | 1 << bit).asInstanceOf[Byte]
    }
    
    val pointerDataLen = if (sorted.forall( sp => sp.data.length == 0 )) 0 else {
      sorted.foldLeft(0)( (accum, sp) => accum + Varint.getUnignedIntEncodingLength(sp.data.length) + sp.data.length)
    }
    
    val totalSize = 1 + 16*2 + sizeLen + idaLen + 1 + indexMaskLen + pointerDataLen
    
    val arr = new Array[Byte](totalSize)
    val bb = ByteBuffer.wrap(arr)
    
    val typeCode = o match {
      case _: DataObjectPointer => DataObjectPointerCode
      case _: KeyValueObjectPointer => KeyValueObjectPointerCode
    }
    
    bb.put(typeCode)
    bb.putLong(o.uuid.getMostSignificantBits)
    bb.putLong(o.uuid.getLeastSignificantBits)
    bb.putLong(o.poolUUID.getMostSignificantBits)
    bb.putLong(o.poolUUID.getLeastSignificantBits)
    Varint.putUnsignedInt(bb, o.size.getOrElse(0))
    o.ida.serializeIDAType(bb)
    bb.put(indexMaskLen.asInstanceOf[Byte])
    bb.put(indexMask)
    if (pointerDataLen != 0) {
      sorted.foreach { sp =>
        Varint.putUnsignedInt(bb, sp.data.length)
        bb.put(sp.data)
      }
    }
    
    arr
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