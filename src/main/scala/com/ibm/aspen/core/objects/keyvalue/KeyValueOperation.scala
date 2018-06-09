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

/* Move this to aspen.core.keyvalue package
 *    Create KeyValueNodeOperation class hierarchy there
 *       - nodeDataToOps(d): List[Ops]
 *       - opsToNodeData( l: List[Ops] ): KeyValueNodeData
 *       - encodeOps(ida: IDA, l: List[Ops]): Array[Array[Byte]]
 *       - decodeOps(ida: IDA, Array[Array[Byte]]): List[Ops]
 *    Create KeyValueNodeData here (ObjectState will contain this)
 * 
 * KeyValue objects support non-conflicting update transactions for independent sets of keys. Transaction
 * requirements are used to ensure that the sequence of updates for a given key are identical across all
 * stores but the sequence of updates across keys may differ between stores. For example the following is
 * a valid set of KeyValue transitions
 * 
 *    Store1: A1 -> B1 -> B2 -> A2 
 *    Store2: B1 -> A1 -> B2 -> B2
 *    
 * What is NOT VALID though would be the following sequence on any store: A2 -> A1
 * 
 * The object data on each store is a series of <update UUID><update-length-varint><update-data> When reading
 * an object, the overall "object revision" used to determine when a consistent set of data has been read from
 * all stores is (ActualObjectRevision, Set[UpdateUUID]). That is to say, each store reports the same overall
 * object revision (which is only modified on select operations, not key-value operations) as well as the same
 * set of applied updates. A set is used for the updates since the specific order of the updates is allowed to
 * differ between stores.
 * 
 * The SetMin, SetMax, SetLeft, SetRight operations require an update to the top-level ObjectRevision. Key-Value
 * updates are checked against the current KeyValue timestamp (or 0 if for non-existent). Key-Value updates may
 * use either exact timestamp comparisons (==) to ensure an exact sequence of updates or may use a weaker 
 * less-than comparison of keyvalue-timestamp < HLCTimestamp.  
 * 
 * The Min, Max, Keys, and Key-timestamps are replicated to each store so they may be used for comparision in
 * transaction requirements. The Left and Right values, as well as the value associated with each key are IDA
 * encoded (of course, this may be a replication IDA but they must be considered opaque as a non-replication
 * IDA may be used). To decode the overall state of the object, each node must be individually decoded into
 * the current Min, Max, EncodedLeft, EncodedRight, and Map[Key, (timestamp, EncodedValueState)]. From these the 
 * final state may be created by using the IDA object do decode each of the encoded values
 * 
 * Binary Encoding strategy
 *   
 *   - Per-store object data consists of a sequence of updates
 *      * <update-uuid><varint-length><data>
 *   
 *   - Ops
 *      * SetMin, SetMax, SetLeft, SetRight
 *         - <val-len><value>
 *      * Insert: Prefix data depends on ObjectPointer options
 *         - <8-byte-timestamp><key-len><value-len><key><value>
 *      * Delete: 
 *         - <key-len><key>
 *      
 */
sealed abstract class KeyValueOperation {
 def getEncodedLength(ida: IDA): Int
 
 def encodeGenericIDA(ida: IDA, bbArray:Array[ByteBuffer]): Unit
 
 /** Replicated IDAs can use a single encoding buffer since identical copies are sent to all
  *  storage nodes. 
  */
 def encodeReplicated(bb: ByteBuffer): Unit
}

// Decoding:
//   Replicated: 
//      list[ByteBuffer] - time-sorted list of appends
//   Generic
//      List[Array[ByteBuffer]] - time-sorted list of appends

object KeyValueOperation {
  val SetMinCode   = 0.asInstanceOf[Byte] // Replicated
  val SetMaxCode   = 1.asInstanceOf[Byte] // Replicated
  val SetLeftCode  = 2.asInstanceOf[Byte] // IDA-encoded
  val SetRightCode = 3.asInstanceOf[Byte] // IDA-encoded
  val InsertCode   = 4.asInstanceOf[Byte] // Replicated Key, timestamp, IDA-encoded value
  val DeleteCode   = 5.asInstanceOf[Byte] // Replicated
  
  trait SingleValueDecoder {
    def create(value: Array[Byte]): KeyValueOperation
    
    def decode(bb: ByteBuffer): KeyValueOperation = {
      val valueLength = Varint.getUnsignedInt(bb)
      val value = new Array[Byte](valueLength)
      bb.get(value)
      create(value)
    }
  }
  
  /** Reads and returns the next KeyValueOperation contained within the provided ByteBuffer
   *  
   *  The position of the buffer is advanced to the end of the operation
   */
  def decode(bb:ByteBuffer): KeyValueOperation = bb.get match {
    case SetMinCode   => SetMin.decode(bb)
    case SetMaxCode   => SetMax.decode(bb)
    case SetLeftCode  => SetLeft.decode(bb)
    case SetRightCode => SetRight.decode(bb)
    case InsertCode   => Insert.decode(bb)
    case DeleteCode   => Delete.decode(bb)
    
    case unknownOpCode => throw new KeyValueObjectEncodingError(new Exception("Unknown KeyValue opcode $unknownOpCode"))
  }
  
  
  def contentToOps(content: Map[Key,Value]): List[KeyValueOperation] = {
    content.valuesIterator.map(v => Insert(v.key.bytes, v.value, v.timestamp)).toList
  }
  
  def insertOperations(content: List[(Key, Array[Byte])], ts: HLCTimestamp): List[Insert] = {
    content.map(t => Insert(t._1, t._2, ts))
  }
  
  def insertOperations(content: List[(Key, Array[Byte])])(implicit tx: Transaction): List[Insert] = {
    content.map(t => Insert(t._1, t._2, tx.timestamp()))
  }
  
  def opsToArray(ops: List[KeyValueOperation]): Array[Byte] = {
    val ida = Replication(1,1)
    val encodedSize = ops.foldLeft(0)( (accum, op) => accum + op.getEncodedLength(ida) )
    val arr = new Array[Byte](encodedSize)
    val bb = ByteBuffer.wrap(arr)
    ops.foreach(op => op.encodeReplicated(bb))
    arr
  }
  def arrayToOps(arr: Array[Byte]): List[KeyValueOperation] = {
    val bb = ByteBuffer.wrap(arr)
    var ops = List[KeyValueOperation]()
    while (bb.remaining() != 0)
      ops = decode(bb) :: ops
    ops
  }
}

sealed abstract class SingleReplicatedValue(val value: Array[Byte]) extends KeyValueOperation {
  
  def opcode: Byte
  
  override def getEncodedLength(ida: IDA): Int = {
    val varintLen = Varint.getUnsignedIntEncodingLength(value.size)
    
    1 + varintLen + value.size
  }
 
  override def encodeGenericIDA(ida: IDA, bbArray:Array[ByteBuffer]): Unit = {
    for (bb <- bbArray) {
      bb.put(opcode)
      Varint.putUnsignedInt(bb, value.size)
      bb.put(value)
    }
  }
  
  override def encodeReplicated(bb: ByteBuffer): Unit = {
    bb.put(opcode)
    Varint.putUnsignedInt(bb, value.size)
    bb.put(value)
  }
}

sealed abstract class SingleEncodedValue(val value: Array[Byte]) extends KeyValueOperation {
  
  def opcode: Byte
  
  override def getEncodedLength(ida: IDA): Int = {
    
    val encLen = ida.calculateEncodedSegmentLength(value)
    val varintLen = Varint.getUnsignedIntEncodingLength(encLen)
    
    1 + varintLen + encLen
  }
 
  override def encodeGenericIDA(ida: IDA, bbArray:Array[ByteBuffer]): Unit = {
    val encLen = ida.calculateEncodedSegmentLength(value)
    for (bb <- bbArray) {
      bb.put(opcode)
      Varint.putUnsignedInt(bb, encLen)
    }
    ida.encodeInto(value, bbArray)
  }
  
  override def encodeReplicated(bb: ByteBuffer): Unit = {
    bb.put(opcode)
    Varint.putUnsignedInt(bb, value.size)
    bb.put(value)
  }
}


class SetMin(value: Key)  extends SingleReplicatedValue(value.bytes) {
  def opcode: Byte = KeyValueOperation.SetMinCode
}
object SetMin extends KeyValueOperation.SingleValueDecoder {
  def apply(value: Array[Byte]) = new SetMin(Key(value))
  def apply(value: Key) = new SetMin(value)
  def create(value: Array[Byte]) = new SetMin(Key(value))
}

class SetMax(value: Key)  extends SingleReplicatedValue(value.bytes) {
  def opcode: Byte = KeyValueOperation.SetMaxCode
}
object SetMax extends KeyValueOperation.SingleValueDecoder {
  def apply(value: Array[Byte]) = new SetMax(Key(value))
  def apply(value: Key) = new SetMax(value)
  def create(value: Array[Byte]) = new SetMax(Key(value))
}

class SetLeft(value: Array[Byte])  extends SingleEncodedValue(value) {
  def opcode: Byte = KeyValueOperation.SetLeftCode
}
object SetLeft extends KeyValueOperation.SingleValueDecoder {
  def apply(value: Array[Byte]) = new SetLeft(value)
  def create(value: Array[Byte]) = new SetLeft(value)
}

class SetRight(value: Array[Byte])  extends SingleEncodedValue(value) {
  def opcode: Byte = KeyValueOperation.SetRightCode
}
object SetRight extends KeyValueOperation.SingleValueDecoder {
  def apply(value: Array[Byte]) = new SetRight(value)
  def create(value: Array[Byte]) = new SetRight(value)
}

class Delete(value: Array[Byte])  extends SingleReplicatedValue(value) {
  def opcode: Byte = KeyValueOperation.DeleteCode
}
object Delete extends KeyValueOperation.SingleValueDecoder {
  def apply(value: Array[Byte]) = new Delete(value)
  def create(value: Array[Byte]) = new Delete(value)
}

/** 
 *  Binary format: <opcode><8-byte-timestamp><key-len><value-len><key><value>
 */
class Insert(
    val key: Array[Byte], 
    val value: Array[Byte], 
    val timestamp: HLCTimestamp) extends KeyValueOperation {
  
  def this(key:Key, value: Array[Byte], timestamp: HLCTimestamp) = this(key.bytes, value, timestamp)
  
  override def getEncodedLength(ida: IDA): Int = {
    val encValLen = ida.calculateEncodedSegmentLength(value)
    
    val keyVarintLen = Varint.getUnsignedIntEncodingLength(key.size)
    val valVarintLen = Varint.getUnsignedIntEncodingLength(encValLen)
    
    1 + 8 + keyVarintLen + valVarintLen + key.size + encValLen
  }
 
  override def encodeGenericIDA(ida: IDA, bbArray:Array[ByteBuffer]): Unit = {
    val encLen = ida.calculateEncodedSegmentLength(value)
    for (bb <- bbArray) {
      bb.put(KeyValueOperation.InsertCode)
      bb.putLong(timestamp.asLong)
      Varint.putUnsignedInt(bb, key.size)
      Varint.putUnsignedInt(bb, encLen)
    }
    ida.encodeInto(value, bbArray)
  }
  
  override def encodeReplicated(bb: ByteBuffer): Unit = {
    bb.put(KeyValueOperation.InsertCode)
    bb.putLong(timestamp.asLong)
    Varint.putUnsignedInt(bb, key.size)
    Varint.putUnsignedInt(bb, value.size)
    bb.put(key)
    bb.put(value)
  }
}
object Insert {
  def apply(key: Array[Byte], value: Array[Byte], timestamp: HLCTimestamp) = new Insert(key, value, timestamp)
  def apply(key: Key, value: Array[Byte], timestamp: HLCTimestamp) = new Insert(key.bytes, value, timestamp)
  
  def decode(bb: ByteBuffer): KeyValueOperation = {
    val timestamp = HLCTimestamp(bb.getLong)
    val key = new Array[Byte](Varint.getUnsignedInt(bb))
    val value = new Array[Byte](Varint.getUnsignedInt(bb))
    bb.get(key)
    bb.get(value)
    new Insert(key, value, timestamp)
  }
}