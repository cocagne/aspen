package com.ibm.aspen.base.kvlist

import com.ibm.aspen.base.{ kvlist => K }
import com.ibm.aspen.core.network.NetworkCodec
import com.google.flatbuffers.FlatBufferBuilder
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer
import scala.collection.immutable.SortedMap

private [base] object KVListCodec {
  
  class KeyOrdering(val keyCompare: (Array[Byte], Array[Byte]) => Int) extends Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]) = keyCompare(a, b)
  }
  
  def decodeNodeContent(
      compareKeys: (Array[Byte], Array[Byte]) => Int, 
      data: ByteBuffer): (SortedMap[Array[Byte], Array[Byte]], Option[KVListNodePointer]) = {
    implicit val keyOrder = new KVListCodec.KeyOrdering(compareKeys)
    val ops = KVListCodec.decodeOperations(data)
    val i: (SortedMap[Array[Byte], Array[Byte]], Option[KVListNodePointer]) = (SortedMap(), None)
    
    ops.foldLeft(i)((t, op) => op match {
      case ins: KVListCodec.Insert => (t._1 + (ins.key -> ins.value), t._2)
      case del: KVListCodec.Delete => (t._1 - del.key, t._2)
      case srp: KVListCodec.SetRightPointer => (t._1, Some(KVListNodePointer(srp.op, srp.minimum)))
    })
  }
  
  def encodeNewListContent(initialContent: List[(Array[Byte], Array[Byte])]): Array[Byte] = {
    val opsList = initialContent.map(t => Insert(t._1, t._2))
    val (appendOps, appendSize, appendOpCount) = KVListCodec.encodeOperations(opsList)
    opsToByteBuffer(appendOps, appendSize).array
  }

  def encodeListDescription(allocationPolicyUUID: UUID, rootObject: ObjectPointer): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)

    val ro = NetworkCodec.encode(builder, rootObject)
    
    K.KVListDescription.startKVListDescription(builder)
    K.KVListDescription.addAllocationPolicyUUID(builder, NetworkCodec.encode(builder, allocationPolicyUUID))
    val tierPointersOffset = K.KVListDescription.addRootObject(builder, ro)

    val m = K.KVListDescription.endKVListDescription(builder)

    builder.finish(m)

    val db = builder.dataBuffer()

    val arr = new Array[Byte](db.capacity - db.position)
    db.get(arr)

    arr
  }

  def decodeListDescription(bb: ByteBuffer): (UUID, ObjectPointer) = {
    val m = K.KVListDescription.getRootAsKVListDescription(bb.asReadOnlyBuffer())
    val allocationPolicyUUID = NetworkCodec.decode(m.allocationPolicyUUID())
    val rootObject = NetworkCodec.decode(m.rootObject())

    (allocationPolicyUUID, rootObject)
  }

  sealed abstract class OpCode {
    def opCode: Byte
    def integerSize: Int
    def encodedSize: Int
    def encodeSize(bb: ByteBuffer, size: Int): Unit
    def decodeSize(bb: ByteBuffer): Int
    def encode(bb: ByteBuffer): Unit
  }

  trait Size1 {
    def integerSize = 1
    def encodeSize(bb: ByteBuffer, size: Int): Unit = bb.put(size.asInstanceOf[Byte])
    def decodeSize(bb: ByteBuffer): Int = bb.get()
  }
  trait Size2 {
    def integerSize = 2
    def encodeSize(bb: ByteBuffer, size: Int): Unit = bb.putShort(size.asInstanceOf[Short])
    def decodeSize(bb: ByteBuffer): Int = bb.getShort()
  }
  trait Size4 {
    def integerSize = 4
    def encodeSize(bb: ByteBuffer, size: Int): Unit = bb.putInt(size.asInstanceOf[Int])
    def decodeSize(bb: ByteBuffer): Int = bb.getInt()
  }

  abstract class KeyedOp extends OpCode {
    def key: Array[Byte]
  }

  abstract class InsertOp extends KeyedOp {
    def value: Array[Byte]
    def encodedSize: Int = 1 + integerSize * 2 + key.length + value.length
    def encode(bb: ByteBuffer) = {
      bb.put(opCode)
      encodeSize(bb, key.length)
      encodeSize(bb, value.length)
      bb.put(key)
      bb.put(value)
    }
  }
  abstract class DeleteOp extends KeyedOp {
    def encodedSize: Int = 1 + integerSize + key.length
    def encode(bb: ByteBuffer) = {
      bb.put(opCode)
      encodeSize(bb, key.length)
      bb.put(key)
    }
  }
  abstract class SetRightPointerOp extends OpCode {
    def pointer: ByteBuffer
    def minimum: Array[Byte]
    def encodedSize: Int = 1 + integerSize * 2 + pointer.capacity() + minimum.length
    def encode(bb: ByteBuffer) = {
      bb.put(opCode)
      encodeSize(bb, pointer.capacity())
      encodeSize(bb, minimum.length)
      pointer.position(0)
      bb.put(pointer)
      bb.put(minimum)
      pointer.position(0)
    }
  }

  case class Insert1(key: Array[Byte], value: Array[Byte]) extends InsertOp with Size1 {
    val opCode: Byte = 0
  }
  case class Insert2(key: Array[Byte], value: Array[Byte]) extends InsertOp with Size2 {
    val opCode: Byte = 1
  }
  case class Insert4(key: Array[Byte], value: Array[Byte]) extends InsertOp with Size4 {
    val opCode: Byte = 2
  }
  case class Delete1(key: Array[Byte]) extends DeleteOp with Size1 {
    val opCode: Byte = 3
  }
  case class Delete2(key: Array[Byte]) extends DeleteOp with Size2 {
    val opCode: Byte = 4
  }
  case class Delete4(key: Array[Byte]) extends DeleteOp with Size4 {
    val opCode: Byte = 5
  }
  case class SetRightPointer1(pointer: ByteBuffer, minimum: Array[Byte]) extends SetRightPointerOp with Size1 {
    val opCode: Byte = 6
  }
  case class SetRightPointer2(pointer: ByteBuffer, minimum: Array[Byte]) extends SetRightPointerOp with Size2 {
    val opCode: Byte = 7
  }
  case class SetRightPointer4(pointer: ByteBuffer, minimum: Array[Byte]) extends SetRightPointerOp with Size4 {
    val opCode: Byte = 8
  }

  //-----------------------------------------------------------------------
  // Logical Operations
  //
  sealed abstract class KVListOperation

  case class Insert(key: Array[Byte], value: Array[Byte]) extends KVListOperation

  case class Delete(key: Array[Byte]) extends KVListOperation

  case class SetRightPointer(op: ObjectPointer, minimum: Array[Byte]) extends KVListOperation

  def decodeOperations(bb: ByteBuffer): List[KVListOperation] = {
    var ops = List[KVListOperation]()

    while (bb.position() < bb.capacity()) 
      ops = decodeOp(bb) :: ops

    ops.reverse
  }

  def decodeOp(bb: ByteBuffer): KVListOperation = {
    def s1() = bb.get()
    def s2() = bb.getShort()
    def s4() = bb.getInt()

    def getInsert(sizeFn: () => Int): Insert = {
      val keyArray = new Array[Byte](sizeFn())
      val valueArray = new Array[Byte](sizeFn())
      bb.get(keyArray)
      bb.get(valueArray)
      Insert(keyArray, valueArray)
    }

    def getDelete(sizeFn: () => Int): Delete = {
      val keyArray = new Array[Byte](sizeFn())
      bb.get(keyArray)
      Delete(keyArray)
    }

    def getSetPointer(sizeFn: () => Int): SetRightPointer = {
      val ptrArray = new Array[Byte](sizeFn())
      val minimumArray = new Array[Byte](sizeFn())
      bb.get(ptrArray)
      bb.get(minimumArray)
      SetRightPointer(NetworkCodec.byteBufferToObjectPointer(ByteBuffer.wrap(ptrArray)), minimumArray)
    }

    bb.get() match {
      case 0 => getInsert(s1 _)
      case 1 => getInsert(s2 _)
      case 2 => getInsert(s4 _)
      case 3 => getDelete(s1 _)
      case 4 => getDelete(s2 _)
      case 5 => getDelete(s4 _)
      case 6 => getSetPointer(s1 _)
      case 7 => getSetPointer(s2 _)
      case 8 => getSetPointer(s4 _)
    }
  }

  def bytesNeeded(arr: Array[Byte]): Int = bytesNeeded(arr.length)
  def bytesNeeded(buf: ByteBuffer): Int = bytesNeeded(buf.capacity())
  def bytesNeeded(dataSize: Int): Int = if (dataSize <= Byte.MaxValue)
    1
  else if (dataSize <= Short.MaxValue)
    2
  else if (dataSize <= Int.MaxValue)
    4
  else
    8

  def mkInsert(key: Array[Byte], value: Array[Byte]): InsertOp = java.lang.Math.max(bytesNeeded(key), bytesNeeded(value)) match {
    case 1 => Insert1(key, value)
    case 2 => Insert2(key, value)
    case 4 => Insert4(key, value)
    case _ => throw new EncodingSizeError
  }

  def mkDelete(key: Array[Byte]): DeleteOp = bytesNeeded(key) match {
    case 1 => Delete1(key)
    case 2 => Delete2(key)
    case 4 => Delete4(key)
    case _ => throw new EncodingSizeError
  }

  def mkSetRight(pointer: ByteBuffer, minimum: Array[Byte]): SetRightPointerOp = {

    java.lang.Math.max(bytesNeeded(pointer), bytesNeeded(minimum)) match {
      case 1 => SetRightPointer1(pointer, minimum)
      case 2 => SetRightPointer2(pointer, minimum)
      case 4 => SetRightPointer4(pointer, minimum)
      case _ => throw new EncodingSizeError
    }
  }

  def mkSetRight(pointer: ObjectPointer, minimum: Array[Byte]): SetRightPointerOp = mkSetRight(NetworkCodec.objectPointerToByteBuffer(pointer), minimum)

  def mkSetRight(nodePointer: KVListNodePointer): SetRightPointerOp = mkSetRight(nodePointer.objectPointer, nodePointer.minimum)
  
  def encode(operations: List[KVListOperation]): ByteBuffer = {
    val (ops, size, _) = encodeOperations(operations)
    opsToByteBuffer(ops, size)
  }
  
  /** For unit tests */
  def testEncodeContent(content: List[(Array[Byte],Array[Byte])], rptr: Option[KVListNodePointer]): ByteBuffer = {
    val ins = content.map(t => Insert(t._1, t._2))
    val ops = rptr match {
      case None => ins
      case Some(rp) => SetRightPointer(rp.objectPointer, rp.minimum) :: ins
    }
    encode(ops)
  }

  /** Returns (List[OpCode], sizeInBytes, opCount) */
  def encodeOperations(operations: List[KVListOperation]): (List[OpCode], Int, Int) = {
    var size = 0
    var opCount = 0
    var ops: List[OpCode] = Nil
    operations.reverse.foreach { op =>
      val eop = op match {
        case o: Insert          => mkInsert(o.key, o.value)
        case o: Delete          => mkDelete(o.key)
        case o: SetRightPointer => mkSetRight(NetworkCodec.objectPointerToByteBuffer(o.op), o.minimum)
      }
      size += eop.encodedSize
      opCount += 1
      ops = eop :: ops
    }
    (ops, size, opCount)
  }

  def opsToByteBuffer(ops: List[OpCode], encodedSize: Int): ByteBuffer = {
    val bb = ByteBuffer.allocate(encodedSize)
    ops.foreach(_.encode(bb))
    bb.position(0)
    bb
  }

  /** Returns a sorted list of insert operations and the current right pointer */
  def compact(
    keyCompare: (Array[Byte], Array[Byte]) => Int,
    currentOps: List[KVListOperation],
    newOps: List[KVListOperation]): (List[InsertOp], Option[SetRightPointer]) = {

    val opIter = currentOps.iterator ++ newOps.iterator

    implicit val ordering = new KeyOrdering(keyCompare)
    var inserts = SortedMap[Array[Byte], Insert]()
    var srp: Option[SetRightPointer] = None

    opIter.foreach { op =>
      op match {
        case o: Insert          => inserts += (o.key -> o)
        case d: Delete          => inserts -= d.key
        case s: SetRightPointer => srp = Some(s)
      }
    }

    (inserts.valuesIterator.map(ins => mkInsert(ins.key, ins.value)).toList, srp)
  }
}