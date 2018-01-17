package com.ibm.aspen.core.objects.keyvalue

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.util.Varint
import java.util.UUID
import java.nio.ByteBuffer
import com.ibm.aspen.core.ida.Replication

class KeyValueObjectState(
    val minimum: Option[Array[Byte]],
    val maximum: Option[Array[Byte]],
    val left: Option[Array[Byte]],
    val right: Option[Array[Byte]],
    val contents: Map[Array[Byte], KVState]) {
  
  /** Encodes the state of a KeyValueObject for sending to DataStores.
   *
   * Data contained within the objects is a series of top-level "update blocks" which
   * consist of <16-byte-update-uuid><varint-length><data>. This method is used to
   * generate a full-state dump of the object and should be used in an overwrite transaction
   * that sets the state of the object to this single value.
   */
  def encode(ida: IDA): Array[DataBuffer] = {
    var ops: List[KeyValueOperation] = Nil
    
    minimum.foreach( arr => ops = new SetMin(arr) :: ops )
    maximum.foreach( arr => ops = new SetMax(arr) :: ops )
    left.foreach( arr => ops = new SetLeft(arr) :: ops )
    right.foreach( arr => ops = new SetRight(arr) :: ops )
    
    contents.valuesIterator.foreach { kv =>
      ops = new Insert(kv.key, kv.value, kv.timestamp) :: ops
    }
    
    KeyValueObjectState.encodeUpdate(ida, ops)
  }
}
    
object KeyValueObjectState {
  //def restore(segments: List[(Byte,Option[DataBuffer])]): DataBuffer
  def apply(storeStates: List[KeyValueObjectStoreState]): KeyValueObjectState = {
    val ida = storeStates.head.ida
    val minimum = storeStates.head.minimum
    val maximum = storeStates.head.maximum
    
    // All "left" and "right" must either be Some or None
    val left = if (storeStates.head.idaEncodedLeft.isDefined) {
      val segments = storeStates.foldLeft(List[(Byte,Option[DataBuffer])]()) { (l, ss) =>
        val db = DataBuffer(ss.idaEncodedLeft.get)
        (ss.idaEncodingIndex, Some(db)) :: l 
      }
      Some(ida.restoreToArray(segments))
    } else
      None
    val right = if (storeStates.head.idaEncodedRight.isDefined) {
      val segments = storeStates.foldLeft(List[(Byte,Option[DataBuffer])]()) { (l, ss) =>
        val db = DataBuffer(ss.idaEncodedRight.get)
        (ss.idaEncodingIndex, Some(db)) :: l 
      }
      Some(ida.restoreToArray(segments))
    } else
      None
    
    var contents = Map[Array[Byte], KVState]()
    
    storeStates.head.idaEncodedContents.foreach { t =>
      val (key, ekv) = t
      val segments = storeStates.map( ss => (ss.idaEncodingIndex, Some(DataBuffer(ss.idaEncodedContents(key).value))) )
      val kv = KVState(key, ida.restoreToArray(segments), ekv.timestamp)
      contents += (key -> kv)
    }
    
    new KeyValueObjectState(minimum, maximum, left, right, contents)
  }
  
  /** Encodes a list of updates to the state of a KeyValueObject for sending to DataStores.
   *
   * Data contained within the objects is a series of top-level "update blocks" which
   * consist of <16-byte-update-uuid><varint-length><data>. This method is used to
   * generate a incremental update to the state of an object and should be used in a
   * transaction that appends the update block to the store state.
   */
  def encodeUpdate(ida: IDA, ops: List[KeyValueOperation]): Array[DataBuffer] = {
    
    val encodedSize = ops.foldLeft(0)( (accum, op) => accum + op.getEncodedLength(ida) )
    val lenVarInt = Varint.getUnignedIntEncodingLength(encodedSize)
    val updateUUID = UUID.randomUUID()
    
    val bbArray = new Array[ByteBuffer](ida.width)
    
    ida match {
      case _: Replication =>
        // Special-case replication to take advantage of the fact that a single buffer 
        // can be shared across all stores
        val bb = ByteBuffer.allocate(16 + lenVarInt + encodedSize)
          
        bb.putLong(updateUUID.getMostSignificantBits)
        bb.putLong(updateUUID.getLeastSignificantBits)
        Varint.putUnsignedInt(bb, encodedSize)
        
        ops.foreach(op => op.encodeReplicated(bb))
        
        bb.position(0)
        
        // Create read-only copies to ensure each buffer has an independent position & limit
        for (i <- 0 until bbArray.size)
          bbArray(i) = bb.asReadOnlyBuffer()
        
      case _ =>
        for (i <- 0 until bbArray.size) {
          bbArray(i) = ByteBuffer.allocate(16 + lenVarInt + encodedSize)
          bbArray(i).putLong(updateUUID.getMostSignificantBits)
          bbArray(i).putLong(updateUUID.getLeastSignificantBits)
          Varint.putUnsignedInt(bbArray(i), encodedSize)
        }
        ops.foreach(op => op.encodeGenericIDA(ida, bbArray))
        
        for (i <- 0 until bbArray.size)
          bbArray(i).position(0)
    }
    
    bbArray.map(DataBuffer(_))
  }
}