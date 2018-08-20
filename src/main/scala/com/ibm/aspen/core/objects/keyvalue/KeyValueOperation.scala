package com.ibm.aspen.core.objects.keyvalue

import java.nio.ByteBuffer
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.util.Varint
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.DataBuffer

abstract class KeyValueOperation {
  
  import KeyValueOperation._
  
  def timestamp: Option[HLCTimestamp]
  
  def revision: Option[ObjectRevision]
  
  protected def opcode: Byte
  
  def replicationOnly: Boolean = false
  
  final def getEncodedLength(ida: IDA): Int = {
    val vlen = if (replicationOnly) getDataLength() else ida.calculateEncodedSegmentLength(getDataLength())
    1 + revision.map(_ => 16).getOrElse(0) + timestamp.map(_ => 8).getOrElse(0) + Varint.getUnsignedIntEncodingLength(vlen) + vlen
  }
  
  // Returns the size of the encoded data payload
  protected def getDataLength(): Int
  
  protected def getData(): DataBuffer
  
  final protected def encodeGenericIda(ida: IDA, bbArray:Array[ByteBuffer]): Unit = {
    if (replicationOnly) {
      val data = getData()
      for (bb <- bbArray)
        bb.put(data.asReadOnlyBuffer())
    }
    else {
      val mask = revision.map(_ => HasRevisionMask).getOrElse(0.asInstanceOf[Byte]) | timestamp.map(_ => HasTimestampMask).getOrElse(0.asInstanceOf[Byte])
      val dataLen = ida.calculateEncodedSegmentLength(getDataLength())
      
      for (bb <- bbArray) {
        bb.put( (mask | opcode).asInstanceOf[Byte] )
        revision.foreach(rev => rev.encodeInto(bb))
        timestamp.foreach(ts => bb.putLong(ts.asLong))
        Varint.putUnsignedInt(bb, dataLen)
      }
      
      val earr = ida.encode(getData())
      for (i <- 0 until bbArray.length)
        bbArray(i).put( earr(i).asReadOnlyBuffer() )
    }
  }
  
  final protected def encodeReplicated(bb: ByteBuffer): Unit = {
    val mask = revision.map(_ => HasRevisionMask).getOrElse(0.asInstanceOf[Byte]) | timestamp.map(_ => HasTimestampMask).getOrElse(0.asInstanceOf[Byte])
    val dataLen = getDataLength()
    bb.put( (mask | opcode).asInstanceOf[Byte] )
    revision.foreach(rev => rev.encodeInto(bb))
    timestamp.foreach(ts => bb.putLong(ts.asLong))
    Varint.putUnsignedInt(bb, dataLen)
    bb.put( getData().asReadOnlyBuffer() )
  }
}

/**
 * Encoding Format:
 *    Sequence of: <code>[16-byte object-revision][8-byte timestamp]<varint-data-len><data>
 *    
 *    <code> is a bitmask with the highest bit being "Has Revision" and second highest being "Has Timestamp". The
 *           remainder is the kind of encoded entry.
 */
object KeyValueOperation {
  val HasRevisionMask = (1 << 7).asInstanceOf[Byte]
  val HasTimestampMask = (1 << 6).asInstanceOf[Byte]
  val CodeMask = 0xFF & ~(HasRevisionMask | HasTimestampMask)
  
  val SetMinCode      = 0.asInstanceOf[Byte] // Replicated
  val SetMaxCode      = 1.asInstanceOf[Byte] // Replicated
  val SetLeftCode     = 2.asInstanceOf[Byte] // IDA-encoded
  val SetRightCode    = 3.asInstanceOf[Byte] // IDA-encoded
  val InsertCode      = 4.asInstanceOf[Byte] // IDA-encoded value
  val DeleteCode      = 5.asInstanceOf[Byte] // Not stored
  val DeleteMinCode   = 6.asInstanceOf[Byte] // Not stored
  val DeleteMaxCode   = 7.asInstanceOf[Byte] // Not stored
  val DeleteLeftCode  = 8.asInstanceOf[Byte] // Not stored
  val DeleteRightCode = 9.asInstanceOf[Byte] // Not stored
  
  def getArray(bb: ByteBuffer, nbytes: Int): Array[Byte] = {
    val arr = new Array[Byte](nbytes)
    bb.get(arr)
    arr
  }
  
  /** Reads and returns the list of KeyValueOperations contained within the provided ByteBuffer
   *  
   *  The position of the buffer is advanced to the end of the operation
   */
  def decode(bb:ByteBuffer, txRevision: ObjectRevision, txTimestamp: HLCTimestamp): List[KeyValueOperation] = {
    var rlist: List[KeyValueOperation] = Nil
    
    while (bb.remaining() != 0) {
      val mask = bb.get()
      
      val rev = if ((mask & HasRevisionMask) == 0) txRevision else ObjectRevision(bb)
      val ts = if ((mask & HasTimestampMask) == 0) txTimestamp else HLCTimestamp(bb.getLong())
      val code = mask & CodeMask
      val dataLen = Varint.getUnsignedInt(bb)
      
      val op = code match {
        case SetMinCode      => SetMin.decode(bb, dataLen, rev, ts)
        case SetMaxCode      => SetMax.decode(bb, dataLen, rev, ts)
        case SetLeftCode     => SetLeft.decode(bb, dataLen, rev, ts)
        case SetRightCode    => SetRight.decode(bb, dataLen, rev, ts)
        case InsertCode      => Insert.decode(bb, dataLen, rev, ts)
        case DeleteCode      => Delete.decode(bb, dataLen)
        case DeleteMinCode   => DeleteMin.decode(bb, dataLen)
        case DeleteMaxCode   => DeleteMax.decode(bb, dataLen)
        case DeleteLeftCode  => DeleteLeft.decode(bb, dataLen)
        case DeleteRightCode => DeleteRight.decode(bb, dataLen)
        
        case unknownOpCode => throw new KeyValueObjectEncodingError(new Exception("Unknown KeyValue opcode $unknownOpCode"))
      }
      
      rlist = op :: rlist 
    }
    
    rlist.reverse
  }
  
  def encode(ops: List[KeyValueOperation], ida: IDA): Array[DataBuffer] = {
    val result = new Array[DataBuffer](ida.width)
    
    val sz = ops.foldLeft(0)( (sz, op) => sz + op.getEncodedLength(ida))
    
    ida match {
      case _: Replication => 
        val arr = new Array[Byte](sz)
        val db = DataBuffer(arr)
        val bb = ByteBuffer.wrap(arr)
        for (i <- 0 until result.length)
          result(i) = db
        ops.foreach(op => op.encodeReplicated(bb))
        
      case _ =>
        val bbs = new Array[ByteBuffer](result.length)
        for (i <- 0 until result.length) {
          val arr = new Array[Byte](sz)
          result(i) = DataBuffer(arr)
          bbs(i) = ByteBuffer.wrap(arr)
        }
        ops.foreach(op => op.encodeGenericIda(ida, bbs))
    }
    
    result
  }
}

sealed abstract class NoValue extends KeyValueOperation {
  
  override def replicationOnly: Boolean = true
  
  protected def getDataLength(): Int = 0
  
  protected def getData(): DataBuffer = DataBuffer.Empty
}

sealed abstract class SingleReplicatedValue(val value: Array[Byte]) extends KeyValueOperation {
  
  override def replicationOnly: Boolean = true
  
  protected def getDataLength(): Int = value.length
  
  protected def getData(): DataBuffer = DataBuffer(value)
}

sealed abstract class SingleEncodedValue(val value: Array[Byte]) extends KeyValueOperation {
  
  protected def getDataLength(): Int = value.length
  
  protected def getData(): DataBuffer = DataBuffer(value)
}

class SetMin(value: Key, val timestamp: Option[HLCTimestamp]=None, val revision: Option[ObjectRevision]=None)  extends SingleReplicatedValue(value.bytes) {
  def opcode: Byte = KeyValueOperation.SetMinCode
}
object SetMin {
  def apply(value: Key, timestamp: Option[HLCTimestamp]=None, revision: Option[ObjectRevision]=None) = new SetMin(value, timestamp, revision)
  def decode(bb: ByteBuffer, dataLen: Int, revision: ObjectRevision, timestamp: HLCTimestamp) = {
    new SetMin(Key(KeyValueOperation.getArray(bb, dataLen)), Some(timestamp), Some(revision))
  }
}

class SetMax(value: Key, val timestamp: Option[HLCTimestamp]=None, val revision: Option[ObjectRevision]=None)  extends SingleReplicatedValue(value.bytes) {
  def opcode: Byte = KeyValueOperation.SetMaxCode
}
object SetMax {
  def apply(value: Key, timestamp: Option[HLCTimestamp]=None, revision: Option[ObjectRevision]=None) = new SetMax(value, timestamp, revision)
  def decode(bb: ByteBuffer, dataLen: Int, revision: ObjectRevision, timestamp: HLCTimestamp) = {
    new SetMax(Key(KeyValueOperation.getArray(bb, dataLen)), Some(timestamp), Some(revision))
  }
}

class SetLeft(value: Array[Byte], val timestamp: Option[HLCTimestamp]=None, val revision: Option[ObjectRevision]=None)  extends SingleEncodedValue(value) {
  def opcode: Byte = KeyValueOperation.SetLeftCode
}
object SetLeft {
  def apply(value: Array[Byte], timestamp: Option[HLCTimestamp]=None, revision: Option[ObjectRevision]=None) = new SetLeft(value, timestamp, revision)
  def decode(bb: ByteBuffer, dataLen: Int, revision: ObjectRevision, timestamp: HLCTimestamp) = {
    new SetLeft(KeyValueOperation.getArray(bb, dataLen), Some(timestamp), Some(revision))
  }
}

class SetRight(value: Array[Byte], val timestamp: Option[HLCTimestamp]=None, val revision: Option[ObjectRevision]=None)  extends SingleEncodedValue(value) {
  def opcode: Byte = KeyValueOperation.SetRightCode
}
object SetRight {
  def apply(value: Array[Byte], timestamp: Option[HLCTimestamp]=None, revision: Option[ObjectRevision]=None) = new SetRight(value, timestamp, revision)
  def decode(bb: ByteBuffer, dataLen: Int, revision: ObjectRevision, timestamp: HLCTimestamp) = {
    new SetRight(KeyValueOperation.getArray(bb, dataLen), Some(timestamp), Some(revision))
  }
}

class Delete(val key: Key)  extends SingleReplicatedValue(key.bytes) {
  def opcode: Byte = KeyValueOperation.DeleteCode
  def timestamp: Option[HLCTimestamp] = None
  def revision: Option[ObjectRevision] = None
}
object Delete {
  def apply(key: Key): Delete = new Delete(key)
  def apply(value: Array[Byte]) = new Delete(Key(value))
  def decode(bb: ByteBuffer, dataLen: Int) = {
    new Delete(Key(KeyValueOperation.getArray(bb, dataLen)))
  }
}

class Insert(
    val key: Key, 
    val value: Array[Byte], 
    val timestamp: Option[HLCTimestamp] = None,
    val revision: Option[ObjectRevision] = None) extends KeyValueOperation {
  
  def opcode: Byte = KeyValueOperation.InsertCode
  
  protected def getDataLength(): Int = Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length + value.length
  
  protected def getData(): DataBuffer = {
    val arr = new Array[Byte](getDataLength)
    val bb = ByteBuffer.wrap(arr)
    Varint.putUnsignedInt(bb, key.bytes.length)
    bb.put(key.bytes)
    bb.put(value)
    arr
  }
}
object Insert {
  def apply(key: Key, value: Array[Byte], timestamp: Option[HLCTimestamp]=None, revision: Option[ObjectRevision]=None) = new Insert(key, value, timestamp, revision)
  
  def decode(bb: ByteBuffer, dataLen: Int, revision: ObjectRevision, timestamp: HLCTimestamp) = {
    val keyLen = Varint.getUnsignedInt(bb)
    val valLen = dataLen - keyLen
    val key = Key(KeyValueOperation.getArray(bb, keyLen))
    val value = KeyValueOperation.getArray(bb, valLen)
    new Insert(key, value, Some(timestamp), Some(revision))
  }
}

class DeleteMin extends NoValue {
  def opcode: Byte = KeyValueOperation.DeleteMinCode
  def timestamp: Option[HLCTimestamp] = None
  def revision: Option[ObjectRevision] = None
}
object DeleteMin {
  def apply(): DeleteMin = new DeleteMin()
  def decode(bb: ByteBuffer, dataLen: Int) = {
    new DeleteMin()
  }
}

class DeleteMax extends NoValue {
  def opcode: Byte = KeyValueOperation.DeleteMaxCode
  def timestamp: Option[HLCTimestamp] = None
  def revision: Option[ObjectRevision] = None
}
object DeleteMax {
  def apply(): DeleteMax = new DeleteMax()
  def decode(bb: ByteBuffer, dataLen: Int) = {
    new DeleteMax()
  }
}

class DeleteRight extends NoValue {
  def opcode: Byte = KeyValueOperation.DeleteRightCode
  def timestamp: Option[HLCTimestamp] = None
  def revision: Option[ObjectRevision] = None
}
object DeleteRight {
  def apply(): DeleteRight = new DeleteRight()
  def decode(bb: ByteBuffer, dataLen: Int) = {
    new DeleteRight()
  }
}

class DeleteLeft extends NoValue {
  def opcode: Byte = KeyValueOperation.DeleteLeftCode
  def timestamp: Option[HLCTimestamp] = None
  def revision: Option[ObjectRevision] = None
}
object DeleteLeft {
  def apply(): DeleteLeft = new DeleteLeft()
  def decode(bb: ByteBuffer, dataLen: Int) = {
    new DeleteLeft()
  }
}