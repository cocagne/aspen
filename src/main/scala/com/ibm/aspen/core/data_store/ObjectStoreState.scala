package com.ibm.aspen.core.data_store

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.objects.keyvalue._
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.objects.{KeyValueObjectState, ObjectRefcount, ObjectRevision}
import com.ibm.aspen.util.Varint

sealed abstract class ObjectStoreState

class DataObjectStoreState(val data: DataBuffer) extends ObjectStoreState

class KeyValueObjectStoreState(val minimum: Option[KeyValueObjectStoreState.Min],
                               val maximum: Option[KeyValueObjectStoreState.Max],
                               val left: Option[KeyValueObjectStoreState.Left],
                               val right: Option[KeyValueObjectStoreState.Right],
                               val idaEncodedContents: Map[Key, Value]) extends ObjectStoreState {

  import KeyValueObjectStoreState._

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

  def update(db: DataBuffer, txRevision: ObjectRevision, txTimestamp: HLCTimestamp): KeyValueObjectStoreState = {

    var tmin: Option[KeyValueObjectStoreState.Min] = minimum
    var tmax: Option[KeyValueObjectStoreState.Max] = maximum
    var tleft: Option[KeyValueObjectStoreState.Left] = left
    var tright: Option[KeyValueObjectStoreState.Right] = right
    var tpairs: Map[Key,Value] = idaEncodedContents

    KeyValueOperation.decode(db.asReadOnlyBuffer(), txRevision, txTimestamp).foreach {
      case op: SetMin   => tmin = Some(Min(Key(op.value), op.revision.get, op.timestamp.get))
      case op: SetMax   => tmax = Some(Max(Key(op.value), op.revision.get, op.timestamp.get))
      case op: SetLeft  => tleft = Some(Left(op.value, op.revision.get, op.timestamp.get))
      case op: SetRight => tright = Some(Right(op.value, op.revision.get, op.timestamp.get))
      case op: Insert   =>
        val v = Value(op.key, op.value, op.timestamp.get, op.revision.get)
        tpairs += (op.key -> v)
      case op: Delete   => tpairs -= op.key
      case _: DeleteMin => tmin = None
      case _: DeleteMax => tmax = None
      case _: DeleteLeft => tleft = None
      case _: DeleteRight => tright = None
    }

    new KeyValueObjectStoreState(tmin, tmax, tleft, tright, tpairs)
  }

  def allUpdates: Set[ObjectRevision] = {
    val i = minimum.map(_.revision).iterator ++
      maximum.map(_.revision).iterator ++
      left.map(_.revision).iterator ++
      right.map(_.revision).iterator ++
      idaEncodedContents.iterator.map(_._2.revision)

    i.foldLeft(Set[ObjectRevision]())( (s,r) => s + r )
  }

  def lastUpdateTimestamp: HLCTimestamp = {
    val i = minimum.map(_.timestamp).iterator ++
      maximum.map(_.timestamp).iterator ++
      left.map(_.timestamp).iterator ++
      right.map(_.timestamp).iterator ++
      idaEncodedContents.iterator.map(_._2.timestamp)

    i.foldLeft(HLCTimestamp(0))( (maxts, ts) =>  if (ts > maxts) ts else maxts)
  }

  def encode(): DataBuffer = KeyValueObjectStoreState.encode(this)

  def encodedSize: Int = KeyValueObjectStoreState.encodedSize(this)
}

/** On disk format is a series of entries:
  *     <code><16-byte-revision><8-byte-timestamp><varint-data-len><data>
  *
  *     Key-Value pair data format: <varint-key-len><key><value>
  *
  */
object KeyValueObjectStoreState {

  case class Min(key: Key, revision: ObjectRevision, timestamp: HLCTimestamp)
  case class Max(key: Key, revision: ObjectRevision, timestamp: HLCTimestamp)
  case class Left(idaEncodedContent: Array[Byte], revision: ObjectRevision, timestamp: HLCTimestamp)
  case class Right(idaEncodedContent: Array[Byte], revision: ObjectRevision, timestamp: HLCTimestamp)

  val CodeMin: Byte   = 0.asInstanceOf[Byte]
  val CodeMax: Byte   = 1.asInstanceOf[Byte]
  val CodeLeft: Byte  = 2.asInstanceOf[Byte]
  val CodeRight: Byte = 3.asInstanceOf[Byte]
  val CodePair: Byte  = 4.asInstanceOf[Byte]

  def encodedEntrySize(nbytes: Int): Int = 1 + 16 + 8 + Varint.getUnsignedIntEncodingLength(nbytes) + nbytes
  def encodedEntrySize(arr: Array[Byte]): Int = encodedEntrySize(arr.length)

  def encodedMinimumSize(min: Key): Int = encodedEntrySize(min.bytes)
  def encodedMaximumSize(min: Key): Int = encodedEntrySize(min.bytes)
  def encodedLeftSize(ida: IDA, left: Array[Byte]): Int = encodedEntrySize(ida.calculateEncodedSegmentLength(left.length))
  def encodedRightSize(ida: IDA, right: Array[Byte]): Int = encodedEntrySize(ida.calculateEncodedSegmentLength(right.length))

  def encodedPairSize(key: Key, value: Array[Byte]): Int = {
    encodedEntrySize(key.bytes.length) + Varint.getUnsignedIntEncodingLength(value.length) + value.length
  }

  def idaEncodedPairSize(ida: IDA, key: Key, value: Array[Byte]): Int = {
    val vlen = ida.calculateEncodedSegmentLength(value.length)
    encodedEntrySize(key.bytes.length) + Varint.getUnsignedIntEncodingLength(vlen) + vlen
  }

  def encodedPairsSize(pairs: Map[Key,Value]): Int = pairs.foldLeft(0)((sz, t) => sz + encodedPairSize(t._1, t._2.value))

  def encodedSize(kvoss: KeyValueObjectStoreState): Int = {
    kvoss.minimum.map(m => encodedEntrySize(m.key.bytes)).getOrElse(0) +
      kvoss.maximum.map(m => encodedEntrySize(m.key.bytes)).getOrElse(0) +
      kvoss.left.map(l => encodedEntrySize(l.idaEncodedContent)).getOrElse(0) +
      kvoss.right.map(r => encodedEntrySize(r.idaEncodedContent)).getOrElse(0) +
      encodedPairsSize(kvoss.idaEncodedContents)
  }

  def encodeEntry(bb: ByteBuffer)(code: Byte, revision: ObjectRevision, timestamp: HLCTimestamp, data: Array[Byte]): Unit = {
    bb.put(code)
    revision.encodeInto(bb)
    bb.putLong(timestamp.asLong)
    Varint.putUnsignedInt(bb, data.length)
    bb.put(data)
  }
  def encodePair(bb: ByteBuffer)(revision: ObjectRevision, timestamp: HLCTimestamp, key: Array[Byte], value: Array[Byte]): Unit = {
    bb.put(CodePair)
    revision.encodeInto(bb)
    bb.putLong(timestamp.asLong)
    Varint.putUnsignedInt(bb, key.length)
    bb.put(key)
    Varint.putUnsignedInt(bb, value.length)
    bb.put(value)
  }

  def apply(): KeyValueObjectStoreState = new KeyValueObjectStoreState(None, None, None, None, Map())

  def apply(db: DataBuffer): KeyValueObjectStoreState = {

    var min: Option[KeyValueObjectStoreState.Min] = None
    var max: Option[KeyValueObjectStoreState.Max] = None
    var left: Option[KeyValueObjectStoreState.Left] = None
    var right: Option[KeyValueObjectStoreState.Right] = None
    var pairs: Map[Key,Value] = Map()

    val bb = db.asReadOnlyBuffer()

    def getArray(nbytes: Int): Array[Byte] = {
      val arr = new Array[Byte](nbytes)
      bb.get(arr)
      arr
    }

    def readEntryHeader(): (Byte, ObjectRevision, HLCTimestamp, Int) = {
      val code = bb.get()
      val msb = bb.getLong()
      val lsb = bb.getLong()
      val timestamp = bb.getLong()
      val dataLen = Varint.getUnsignedInt(bb)
      (code, ObjectRevision(new UUID(msb, lsb)), HLCTimestamp(timestamp), dataLen)
    }

    while (bb.remaining() > 0) {
      val (code, revision, timestamp, dataLen) = readEntryHeader()

      code match {
        case CodeMin => min = Some(Min(Key(getArray(dataLen)), revision, timestamp))
        case CodeMax => max = Some(Max(Key(getArray(dataLen)), revision, timestamp))
        case CodeLeft => left = Some(Left(getArray(dataLen), revision, timestamp))
        case CodeRight => right = Some(Right(getArray(dataLen), revision, timestamp))
        case CodePair =>
          val key = Key(getArray(dataLen))
          val valueLen = Varint.getUnsignedInt(bb)
          val value = getArray(valueLen)
          pairs += (key -> Value(key, value, timestamp, revision))
      }
    }

    new KeyValueObjectStoreState(min, max, left, right, pairs)
  }

  def encode(kvoss: KeyValueObjectStoreState): DataBuffer = {
    val arr = new Array[Byte](encodedSize(kvoss))
    val bb = ByteBuffer.wrap(arr)

    val eentry = encodeEntry(bb) _
    val epair = encodePair(bb) _

    kvoss.minimum.foreach(m => eentry(CodeMin, m.revision, m.timestamp, m.key.bytes))
    kvoss.maximum.foreach(m => eentry(CodeMax, m.revision, m.timestamp, m.key.bytes))
    kvoss.left.foreach(l => eentry(CodeLeft, l.revision, l.timestamp, l.idaEncodedContent))
    kvoss.right.foreach(r => eentry(CodeRight, r.revision, r.timestamp, r.idaEncodedContent))
    kvoss.idaEncodedContents.valuesIterator.foreach( v => epair(v.revision, v.timestamp, v.key.bytes, v.value) )

    DataBuffer(arr)
  }

  def getRebuildState(ida: IDA,
                      idaIndex: Int,
                      kvos: KeyValueObjectState): KeyValueObjectStoreState = {
    val min = kvos.minimum.map(m => Min(m.key, m.revision, m.timestamp))
    val max = kvos.maximum.map(m => Max(m.key, m.revision, m.timestamp))
    val lft = kvos.left.map( m => Left(ida.encode(m.content)(idaIndex), m.revision, m.timestamp))
    val rht = kvos.right.map( m => Right(ida.encode(m.content)(idaIndex), m.revision, m.timestamp))
    val cnt = kvos.contents.map(t => t._1 -> Value(t._1, ida.encode(t._2.value)(idaIndex), t._2.timestamp, t._2.revision))

    new KeyValueObjectStoreState(min, max, lft, rht, cnt)
  }
}