package com.ibm.aspen.core.objects

import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import java.util.UUID

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
    override val pointer: DataObjectPointer, 
    revision:ObjectRevision, 
    refcount:ObjectRefcount, 
    timestamp: HLCTimestamp,
    val sizeOnStore: Int,
    val data: DataBuffer) extends ObjectState(pointer, revision, refcount, timestamp) {
  
  def size: Int = pointer.ida.calculateRestoredObjectSize(sizeOnStore)
  
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
      pointer: DataObjectPointer, 
      revision:ObjectRevision, 
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp,
      sizeOnStore: Int,
      data: DataBuffer): DataObjectState = new DataObjectState(pointer, revision, refcount, timestamp, sizeOnStore, data) 
}

class KeyValueObjectState(
    override val pointer: KeyValueObjectPointer, 
    revision:ObjectRevision,
    refcount:ObjectRefcount, 
    timestamp: HLCTimestamp,
    val updates: Set[UUID],
    val sizeOnStore: Int,
    val minimum: Option[Key],
    val maximum: Option[Key],
    val left: Option[Array[Byte]],
    val right: Option[Array[Byte]],
    val contents: Map[Key, Value]
    ) extends ObjectState(pointer, revision, refcount, timestamp) {
  
  import KeyValueObjectState._
  
  def size: Int = pointer.ida.calculateRestoredObjectSize(sizeOnStore)
  
  def keyInRange(key: Key, ordering: KeyOrdering): Boolean = {
    val minOk = minimum match {
      case None => true
      case Some(min) => ordering.compare(key, min) >= 0
    }
    val maxOk = maximum match {
      case None => true
      case Some(max) => ordering.compare(key, max) < 0
    }
    minOk && maxOk
  }
  
  def canEqual(other: Any): Boolean = other.isInstanceOf[KeyValueObjectState]
  
  override def equals(other: Any): Boolean = {
    other match {
      case that: KeyValueObjectState =>
        (that canEqual this) && pointer == that.pointer && revision == that.revision && refcount == that.refcount && 
        cmpk(minimum,that.minimum) && cmpk(maximum, that.maximum) && cmp(left, that.left) && cmp(right, that.right) && contents == that.contents
      case _ => false
    }
  }
  override def hashCode: Int = {
    def hk(o: Option[Key]): Int = o match {
      case None => 0
      case Some(key) => java.util.Arrays.hashCode(key.bytes)
    }
    def ha(o: Option[Array[Byte]]): Int = o match {
      case None => 0
      case Some(arr) => java.util.Arrays.hashCode(arr)
    }
    val hashCodes = List(pointer.hashCode, revision.hashCode, refcount.hashCode, timestamp.hashCode, hk(minimum), hk(maximum), ha(left), ha(right), contents.hashCode)
    hashCodes.reduce( (a,b) => a ^ b )
  }
  
  override def toString(): String = {
    def p(o:Option[Array[Byte]]): String = o match {
      case None => ""
      case Some(arr) => com.ibm.aspen.util.arr2string(arr)
    }
    def pk(o:Option[Key]): String = o match {
      case None => ""
      case Some(key) => com.ibm.aspen.util.arr2string(key.bytes)
    }
    s"KVObjectState(object: ${pointer.uuid}, revision: $revision, refcount: $refcount, min: ${pk(minimum)}, max: ${pk(maximum)}, left: ${p(left)}, right: ${p(right)}, contents: ${contents}"
  }
}

object KeyValueObjectState {
  def compare(
      pointer: KeyValueObjectPointer, 
      revision:ObjectRevision, 
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp,
      updates: Set[UUID],
      sizeOnStore: Int,
      minimum: Option[Key],
      maximum: Option[Key],
      left: Option[Array[Byte]],
      right: Option[Array[Byte]],
      contents: Map[Key, Value]): KeyValueObjectState = {
    new KeyValueObjectState(pointer, revision, refcount, timestamp, updates, sizeOnStore, minimum, maximum, left, right, contents)
  }
  
  def cmp(a: Option[Array[Byte]], b: Option[Array[Byte]]): Boolean = (a,b) match {
    case (None, None) => true
    case (Some(_), None) => false
    case (None, Some(_)) => false
    case (Some(x), Some(y)) => java.util.Arrays.equals(x, y)
  }
  
  def cmpk(a: Option[Key], b: Option[Key]): Boolean = (a,b) match {
    case (None, None) => true
    case (Some(_), None) => false
    case (None, Some(_)) => false
    case (Some(x), Some(y)) => java.util.Arrays.equals(x.bytes, y.bytes)
  }
}
