package com.ibm.aspen.core.objects

import java.util.UUID

import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.keyvalue.{Key, KeyOrdering, KeyValueObjectStoreState, Value}

sealed abstract class ObjectState(
    val pointer: ObjectPointer, 
    val revision:ObjectRevision, 
    val refcount:ObjectRefcount, 
    val timestamp: HLCTimestamp,
    val readTimestamp: HLCTimestamp) {

  def uuid: UUID = pointer.uuid

  def canEqual(other: Any): Boolean
  
  def getRebuildDataForStore(storeId: DataStoreID): Option[DataBuffer]
  
  def lastUpdateTimestamp: HLCTimestamp
}

class MetadataObjectState(
    pointer: ObjectPointer, 
    revision:ObjectRevision, 
    refcount:ObjectRefcount, 
    timestamp: HLCTimestamp,
    readTimestamp: HLCTimestamp) extends ObjectState(pointer, revision, refcount, timestamp, readTimestamp) {
  
  def lastUpdateTimestamp: HLCTimestamp = timestamp
  
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
  
  def getRebuildDataForStore(storeId: DataStoreID): Option[DataBuffer] = None
}

object MetadataObjectState {
  def apply(
      pointer: ObjectPointer, 
      revision:ObjectRevision, 
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp,
      readTimestamp: HLCTimestamp): MetadataObjectState = new MetadataObjectState(pointer, revision, refcount, timestamp, readTimestamp) 
}
    
class DataObjectState(
    override val pointer: DataObjectPointer, 
    revision:ObjectRevision, 
    refcount:ObjectRefcount, 
    timestamp: HLCTimestamp,
    readTimestamp: HLCTimestamp,
    val sizeOnStore: Int,
    val data: DataBuffer) extends ObjectState(pointer, revision, refcount, timestamp, readTimestamp) {
  
  def lastUpdateTimestamp: HLCTimestamp = timestamp
  
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
  
  def getRebuildDataForStore(storeId: DataStoreID): Option[DataBuffer] = pointer.getEncodedDataIndexForStore(storeId).map { idx => 
    pointer.ida.encode(data)(idx)
  }
}

object DataObjectState {
  def apply(
      pointer: DataObjectPointer, 
      revision:ObjectRevision, 
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp,
      readTimestamp: HLCTimestamp,
      sizeOnStore: Int,
      data: DataBuffer): DataObjectState = new DataObjectState(pointer, revision, refcount, timestamp, readTimestamp, sizeOnStore, data) 
}

class KeyValueObjectState(
    override val pointer: KeyValueObjectPointer, 
    revision:ObjectRevision,
    refcount:ObjectRefcount, 
    timestamp: HLCTimestamp,
    readTimestamp: HLCTimestamp,
    val minimum: Option[KeyValueObjectState.Min],
    val maximum: Option[KeyValueObjectState.Max],
    val left: Option[KeyValueObjectState.Left],
    val right: Option[KeyValueObjectState.Right],
    val contents: Map[Key, Value]
    ) extends ObjectState(pointer, revision, refcount, timestamp, readTimestamp) {
  
  // Provide a rough approximation of the total size consumed on the store
  def sizeOnStore: Int = contents.foldLeft(0)((sz, t) => sz + KeyValueObjectStoreState.idaEncodedPairSize(pointer.ida, t._1, t._2.value)) +
    minimum.map(m => 16 + 8 + 4 + m.key.bytes.length).getOrElse(0) + 
    maximum.map(m => 16 + 8 + 4 + m.key.bytes.length).getOrElse(0) +
    left.map(l => 16 + 8 + 4 + pointer.ida.calculateEncodedSegmentLength(l.content.length)).getOrElse(0) +
    right.map(r => 16 + 8 + 4 + pointer.ida.calculateEncodedSegmentLength(r.content.length)).getOrElse(0)
  
  def guessSizeOnStoreAfterUpdate(inserts: List[(Key,Array[Byte])], deletes: List[Key]): Int = {
    val adds = inserts.foldLeft(0)((sz, t) => sz + KeyValueObjectStoreState.idaEncodedPairSize(pointer.ida, t._1, t._2))
    val dels = deletes.foldLeft(0) { (sz, k) => 
      val x = contents.get(k) match {
        case None => 0
        case Some(v) => KeyValueObjectStoreState.idaEncodedPairSize(pointer.ida, v.key, v.value)
      }
      sz + x
    }
    
    val guess = sizeOnStore + adds - dels
    
    if (guess < 0) 0 else guess
  }
  
  /** Rough approximation. Only considers restored sizes */
  def size: Int = contents.foldLeft(0)((sz, t) => sz + t._1.bytes.length + t._2.value.length) + 
    minimum.map(_.key.bytes.length).getOrElse(0) + 
    maximum.map(_.key.bytes.length).getOrElse(0) +
    left.map(l => l.content.length).getOrElse(0) +
    right.map(r => r.content.length).getOrElse(0)
  
  def keyInRange(key: Key, ordering: KeyOrdering): Boolean = {
    val minOk = minimum match {
      case None => true
      case Some(min) => ordering.compare(key, min.key) >= 0
    }
    val maxOk = maximum match {
      case None => true
      case Some(max) => ordering.compare(key, max.key) < 0
    }
    minOk && maxOk
  }
  
  def canEqual(other: Any): Boolean = other.isInstanceOf[KeyValueObjectState]
  
  override def equals(other: Any): Boolean = {
    other match {
      case that: KeyValueObjectState =>
        val minEq = (minimum, that.minimum) match {
          case (None, None) => true
          case (Some(_), None) => false
          case (None, Some(_)) => false
          case (Some(x), Some(y)) => x.revision == y.revision && x.timestamp == y.timestamp && java.util.Arrays.equals(x.key.bytes, y.key.bytes)
        }
        val maxEq = (maximum, that.maximum) match {
          case (None, None) => true
          case (Some(_), None) => false
          case (None, Some(_)) => false
          case (Some(x), Some(y)) => x.revision == y.revision && x.timestamp == y.timestamp && java.util.Arrays.equals(x.key.bytes, y.key.bytes)
        }
        val leftEq = (left, that.left) match {
          case (None, None) => true
          case (Some(_), None) => false
          case (None, Some(_)) => false
          case (Some(x), Some(y)) => x.revision == y.revision && x.timestamp == y.timestamp && java.util.Arrays.equals(x.content, y.content)
        }
        val rightEq = (left, that.left) match {
          case (None, None) => true
          case (Some(_), None) => false
          case (None, Some(_)) => false
          case (Some(x), Some(y)) => x.revision == y.revision && x.timestamp == y.timestamp && java.util.Arrays.equals(x.content, y.content)
        }
        
        (that canEqual this) && pointer == that.pointer && revision == that.revision && refcount == that.refcount && 
        minEq && maxEq && leftEq && rightEq && contents == that.contents
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
    val hashCodes = List(pointer.hashCode, revision.hashCode, refcount.hashCode, timestamp.hashCode, 
                         hk(minimum.map(_.key)), hk(maximum.map(_.key)), ha(left.map(_.content)), ha(right.map(_.content)), contents.hashCode)
    hashCodes.reduce( (a,b) => a ^ b )
  }
  
  def updateSet: Set[ObjectRevision] = {
    val i = minimum.map(_.revision).iterator ++ 
            maximum.map(_.revision).iterator ++
            left.map(_.revision).iterator ++ 
            right.map(_.revision).iterator ++
            contents.iterator.map(_._2.revision)
            
    i.foldLeft(Set[ObjectRevision]())( (s,r) => s + r )
  }
  
  def lastUpdateTimestamp: HLCTimestamp = {
    val i = minimum.map(_.timestamp).iterator ++ 
            maximum.map(_.timestamp).iterator ++
            left.map(_.timestamp).iterator ++ 
            right.map(_.timestamp).iterator ++
            contents.iterator.map(_._2.timestamp)
            
    i.foldLeft(timestamp)( (maxts, ts) =>  if (ts > maxts) ts else maxts)
  }
  
  override def toString: String = {
    def p(o:Option[Array[Byte]]): String = o match {
      case None => ""
      case Some(arr) => com.ibm.aspen.util.printableArray(arr)
    }
    
    val min = minimum.map(m => com.ibm.aspen.util.printableArray(m.key.bytes)).getOrElse("")
    val max = maximum.map(m => com.ibm.aspen.util.printableArray(m.key.bytes)).getOrElse("")
    val l = left.map(l => com.ibm.aspen.util.printableArray(l.content)).getOrElse("")
    val r = right.map(r => com.ibm.aspen.util.printableArray(r.content)).getOrElse("")
    
    s"KVObjectState(object: ${pointer.uuid}, revision: $revision, refcount: $refcount, min: ${min}, max: ${max}, left: ${l}, right: ${r}, contents: ${contents}"
  }
  
  def getRebuildDataForStore(storeId: DataStoreID): Option[DataBuffer] = pointer.getEncodedDataIndexForStore(storeId).map { idaIndex => 
    KeyValueObjectStoreState.encode(KeyValueObjectStoreState.getRebuildState(pointer.ida, idaIndex, this))
  }
}

object KeyValueObjectState {
  
  case class Min(key: Key, revision: ObjectRevision, timestamp: HLCTimestamp)
  case class Max(key: Key, revision: ObjectRevision, timestamp: HLCTimestamp)
  case class Left(content: Array[Byte], revision: ObjectRevision, timestamp: HLCTimestamp)
  case class Right(content: Array[Byte], revision: ObjectRevision, timestamp: HLCTimestamp)
  
  def compare(
      pointer: KeyValueObjectPointer, 
      revision:ObjectRevision, 
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp,
      readTimestamp: HLCTimestamp,
      minimum: Option[Min],
      maximum: Option[Max],
      left: Option[Left],
      right: Option[Right],
      contents: Map[Key, Value]): KeyValueObjectState = {
    new KeyValueObjectState(pointer, revision, refcount, timestamp, readTimestamp, minimum, maximum, left, right, contents)
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
