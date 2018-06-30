package com.ibm.aspen.core.objects.keyvalue

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.util.Varint
import java.util.UUID
import java.nio.ByteBuffer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.ObjectEncodingError
import com.ibm.aspen.core.objects.KeyValueObjectPointer
    
object KeyValueObjectCodec {
  
  def getUpdateSet(db: DataBuffer): Set[UUID] = {
    var updates = Set[UUID]()
    
    val bb = db.asReadOnlyBuffer()
    
    try {
      while (bb.remaining() != 0) {
        val msb = bb.getLong()
        val lsb = bb.getLong()
        val len = Varint.getUnsignedInt(bb)
        bb.position( bb.position + len )
        updates += new UUID(msb, lsb)
      }
    } catch {
      case t: Throwable => throw new KeyValueObjectEncodingError(t)
    }
    
    updates
  }
  
  def decode(
      pointer: KeyValueObjectPointer, 
      revision:ObjectRevision,
      updates: Set[UUID],
      refcount:ObjectRefcount, 
      timestamp: HLCTimestamp,
      sizeOnStore: Int,
      storeStates: List[KeyValueObjectStoreState]): KeyValueObjectState = {
    try {
      val ida = pointer.ida
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
      
      var contents = Map[Key, Value]()
      
      storeStates.head.idaEncodedContents.foreach { t =>
        val (key, ekv) = t
        val segments = storeStates.map( ss => (ss.idaEncodingIndex, Some(DataBuffer(ss.idaEncodedContents(key).value))) )
        val kv = Value(key, ida.restoreToArray(segments), ekv.timestamp)
        contents += (key -> kv)
      }
      
      new KeyValueObjectState(pointer, revision, refcount, timestamp, updates, sizeOnStore, minimum, maximum, left, right, contents)
    } catch {
      case t: Throwable => throw new KeyValueObjectEncodingError(t)
    }
  }
  
  /** Encodes the state of a KeyValueObject for sending to DataStores.
   *
   * Data contained within the objects is a series of top-level "update blocks" which
   * consist of <16-byte-update-uuid><varint-length><data>. This method is used to
   * generate a full-state dump of the object and should be used in an overwrite transaction
   * that sets the state of the object to this single value.
   * 
   * If updates are provided, empty updates with those UUIDs will be inserted (used for rebuilding
   * out-of-date objects)
   */
  def encode(ida: IDA, kvos: KeyValueObjectState, updates: Set[UUID] = Set()): Array[DataBuffer] = {
    var ops: List[KeyValueOperation] = Nil
    
    kvos.minimum.foreach( arr => ops = new SetMin(arr) :: ops )
    kvos.maximum.foreach( arr => ops = new SetMax(arr) :: ops )
    kvos.left.foreach( arr => ops = new SetLeft(arr) :: ops )
    kvos.right.foreach( arr => ops = new SetRight(arr) :: ops )
    
    kvos.contents.valuesIterator.foreach { kv =>
      ops = new Insert(kv.key.bytes, kv.value, kv.timestamp) :: ops
    }
    
    KeyValueObjectCodec.encodeUpdate(ida, ops, updates)
  }
  
  /** Used on a DataStore to return a subset of the local key-value object state. */
  def encodePartialRead(kvos: KeyValueObjectStoreState, includeMinMax: Boolean, kvlist: List[Value]): DataBuffer = {
    var ops: List[KeyValueOperation] = Nil
    
    if (includeMinMax) {
      kvos.minimum.foreach( arr => ops = new SetMin(arr) :: ops )
      kvos.maximum.foreach( arr => ops = new SetMax(arr) :: ops )
      kvos.idaEncodedLeft.foreach( arr => ops = new SetLeft(arr) :: ops )
      kvos.idaEncodedRight.foreach( arr => ops = new SetRight(arr) :: ops )
    }
    
    kvlist.foreach{ kv => 
      ops = new Insert(kv.key.bytes, kv.value, kv.timestamp) :: ops 
    }
    
    val encodedSize = ops.foldLeft(0)( (accum, op) => accum + op.getEncodedLength(Replication(1,1)) )
    val bb = ByteBuffer.allocate(encodedSize)
    ops.foreach(op => op.encodeReplicated(bb))
        
    bb.position(0)
    DataBuffer(bb)
  }
  
  /** Returns the number of bytes needed to store a list of KeyValueOperations using the provided IDA 
   */
  def calculateEncodedSize(ida: IDA, ops: List[KeyValueOperation]): Int = {
    val encodedDataSize = ops.foldLeft(0)( (accum, op) => accum + op.getEncodedLength(ida) )
    val lenVarInt = Varint.getUnsignedIntEncodingLength(encodedDataSize)
    16 + lenVarInt + encodedDataSize
  }
  
  /** Encodes a list of updates to the state of a KeyValueObject for sending to DataStores.
   *
   * Data contained within the objects is a series of top-level "update blocks" which
   * consist of <16-byte-update-uuid><varint-length><data>. This method is used to
   * generate a incremental update to the state of an object. Overwrite transactions
   * may use this method as well to generate a single update containing the full
   * content of the object.
   */
  def encodeUpdate(ida: IDA, ops: List[KeyValueOperation], updates: Set[UUID] = Set()): Array[DataBuffer] = {
    
    val encodedSize = ops.foldLeft(0)( (accum, op) => accum + op.getEncodedLength(ida) )
    val lenVarInt = Varint.getUnsignedIntEncodingLength(encodedSize)
    val (updateUUID, extraUpdates) = if (updates.isEmpty) (UUID.randomUUID(), Nil) else {
      val ul = updates.toList
      (ul.head, ul.tail)
    }
    
    val bbArray = new Array[ByteBuffer](ida.width)
    
    ida match {
      case _: Replication =>
        // Special-case replication to take advantage of the fact that a single buffer 
        // can be shared across all stores
        val bb = ByteBuffer.allocate(16 + lenVarInt + encodedSize + (extraUpdates.size * (16+Varint.getUnsignedIntEncodingLength(0))))
        
        extraUpdates.foreach { uuid =>
          bb.putLong(uuid.getMostSignificantBits)
          bb.putLong(uuid.getLeastSignificantBits)
          Varint.putUnsignedInt(bb, 0)
        }
        
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
  
  def getUpdates(db: DataBuffer): Set[UUID] = {
    val bb = db.asReadOnlyBuffer()
      
    var updates = Set[UUID]()
    
    while (bb.remaining() > 0) {
      val a = bb.getLong() // Update UUID mostSignificantBits
      val b = bb.getLong() // Update UUID leastSignificantBits
      updates += new UUID(a,b) 
      
      val updateSize = Varint.getUnsignedInt(bb)
      
      bb.position(bb.position + updateSize)
    }
    
    updates
  }
}