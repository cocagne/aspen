package com.ibm.aspen.core.objects

import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.KeyComparison

sealed abstract class ObjectState(
    val pointer: ObjectPointer, 
    val revision:ObjectRevision, 
    val refcount:ObjectRefcount, 
    val timestamp: HLCTimestamp) {
  
  def canEqual(other: Any): Boolean
}

class MetadataObjectState(
    pointer: ObjectPointer, 
    revision:ObjectRevision, 
    refcount:ObjectRefcount, 
    timestamp: HLCTimestamp) extends ObjectState(pointer, revision, refcount, timestamp) {
  
  def canEqual(other: Any): Boolean = other.isInstanceOf[MetadataObjectState]
  
  override def equals(other: Any): Boolean = {
    other match {
      case that: MetadataObjectState => (that canEqual this) && pointer == that.pointer && revision == that.revision && refcount == that.refcount 
      case _ => false
    }
  }
  
  override def hashCode: Int = {
    val hashCodes = List(pointer.hashCode, revision.hashCode, refcount.hashCode, timestamp.hashCode)
    hashCodes.reduce( (a,b) => a ^ b )
  }
}

object MetadataObjectState {
  def apply(
      pointer: ObjectPointer, 
      revision:ObjectRevision, 
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp): MetadataObjectState = new MetadataObjectState(pointer, revision, refcount, timestamp) 
}
    
class DataObjectState(
    pointer: ObjectPointer, 
    revision:ObjectRevision, 
    refcount:ObjectRefcount, 
    timestamp: HLCTimestamp, 
    val data: DataBuffer) extends ObjectState(pointer, revision, refcount, timestamp) {
  
  def canEqual(other: Any): Boolean = other.isInstanceOf[DataObjectState]
  
  override def equals(other: Any): Boolean = {
    other match {
      case that: DataObjectState => (that canEqual this) && pointer == that.pointer && revision == that.revision && refcount == that.refcount && data == that.data
      case _ => false
    }
  }
  
  override def hashCode: Int = {
    val hashCodes = List(pointer.hashCode, revision.hashCode, refcount.hashCode, timestamp.hashCode, data.hashCode)
    hashCodes.reduce( (a,b) => a ^ b )
  }
}

object DataObjectState {
  def apply(
      pointer: ObjectPointer, 
      revision:ObjectRevision, 
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp, 
      data: DataBuffer): DataObjectState = new DataObjectState(pointer, revision, refcount, timestamp, data) 
}

class KeyValueObjectState(
    pointer: ObjectPointer, 
    revision:ObjectRevision, 
    refcount:ObjectRefcount, 
    timestamp: HLCTimestamp,
    val minimum: Option[Array[Byte]],
    val maximum: Option[Array[Byte]],
    val left: Option[Array[Byte]],
    val right: Option[Array[Byte]],
    val contents: Map[Key, Value]
    ) extends ObjectState(pointer, revision, refcount, timestamp) {
  
  import KeyValueObjectState._
  
  def keyInRange(key: Key, compare: KeyComparison): Boolean = {
    val minOk = minimum match {
      case None => true
      case Some(min) => compare(key, min) >= 0
    }
    val maxOk = maximum match {
      case None => true
      case Some(max) => compare(key, max) <= 0
    }
    minOk && maxOk
  }
  
  def canEqual(other: Any): Boolean = other.isInstanceOf[KeyValueObjectState]
  
  override def equals(other: Any): Boolean = {
    other match {
      case that: KeyValueObjectState =>
        (that canEqual this) && pointer == that.pointer && revision == that.revision && refcount == that.refcount && 
        cmp(minimum,that.minimum) && cmp(maximum, that.maximum) && cmp(left, that.left) && cmp(right, that.right) && contents == that.contents
      case _ => false
    }
  }
  override def hashCode: Int = {
    def h(o: Option[Array[Byte]]): Int = o match {
      case None => 0
      case Some(arr) => java.util.Arrays.hashCode(arr)
    }
    val hashCodes = List(pointer.hashCode, revision.hashCode, refcount.hashCode, timestamp.hashCode, h(minimum), h(maximum), h(left), h(right), contents.hashCode)
    hashCodes.reduce( (a,b) => a ^ b )
  }
  
  override def toString(): String = {
    def p(o:Option[Array[Byte]]): String = o match {
      case None => ""
      case Some(arr) => com.ibm.aspen.util.arr2string(arr)
    }
    s"KVObjectState(object: ${pointer.uuid}, revision: $revision, refcount: $refcount, min: ${p(minimum)}, max: ${p(maximum)}, left: ${p(left)}, right: ${p(right)}, contents: ${contents}"
  }
}

object KeyValueObjectState {
  def apply(
      pointer: ObjectPointer, 
      revision:ObjectRevision, 
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp, 
      minimum: Option[Array[Byte]],
      maximum: Option[Array[Byte]],
      left: Option[Array[Byte]],
      right: Option[Array[Byte]],
      contents: Map[Key, Value]): KeyValueObjectState = {
    new KeyValueObjectState(pointer, revision, refcount, timestamp, minimum, maximum, left, right, contents)
  }
  
  def cmp(a: Option[Array[Byte]], b: Option[Array[Byte]]): Boolean = (a,b) match {
    case (None, None) => true
    case (Some(_), None) => false
    case (None, Some(_)) => false
    case (Some(x), Some(y)) => java.util.Arrays.equals(x, y)
  }
}
