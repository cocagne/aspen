package com.ibm.aspen.core.objects.keyvalue

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.util.Varint

/** Represents the decoded object state from a single DataStore.
 */
class KeyValueObjectStoreState(
    val idaEncodingIndex: Byte,
    val minimum: Option[Key],
    val maximum: Option[Key],
    val idaEncodedLeft: Option[Array[Byte]],
    val idaEncodedRight: Option[Array[Byte]],
    val idaEncodedContents: Map[Key, Value]) {
  
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
}
    
object KeyValueObjectStoreState {
  
  def apply(idaEncodingIndex: Byte, db: DataBuffer): KeyValueObjectStoreState = {
    try {
      var minimum: Option[Key] = None
      var maximum: Option[Key] = None
      var left: Option[Array[Byte]] = None
      var right: Option[Array[Byte]] = None
      var contents: Map[Key, Value] = Map()
     
      val bb = db.asReadOnlyBuffer()
      
      while (bb.remaining() > 0) {
        bb.getLong() // Update UUID mostSignificantBits
        bb.getLong() // Update UUID leastSignificantBits
        val updateSize = Varint.getUnsignedInt(bb)
        
        val updateEndPosition = bb.position + updateSize
          
        while (bb.position != updateEndPosition) {
          KeyValueOperation.decode(bb) match {
            case op: SetMin => minimum = Some(Key(op.value))
            case op: SetMax => maximum = Some(Key(op.value))
            case op: SetLeft => left = Some(op.value)
            case op: SetRight => right = Some(op.value)
            case op: Insert => 
              val key = Key(op.key)
              contents += (key -> Value(key, op.value, op.timestamp))
            case op: Delete => contents -= Key(op.value)
          }
        }
        
      }
      
      new KeyValueObjectStoreState(idaEncodingIndex, minimum, maximum, left, right, contents)
    } catch {
      case t: Throwable => throw new KeyValueObjectEncodingError(t)
    }
  }
  
  def decodePartialRead(idaEncodingIndex: Byte, db: DataBuffer): KeyValueObjectStoreState = {
    try {
      var minimum: Option[Key] = None
      var maximum: Option[Key] = None
      var left: Option[Array[Byte]] = None
      var right: Option[Array[Byte]] = None
      var contents: Map[Key, Value] = Map()
     
      val bb = db.asReadOnlyBuffer()
            
      while (bb.remaining() > 0) {
        KeyValueOperation.decode(bb) match {
          case op: SetMin => minimum = Some(Key(op.value))
          case op: SetMax => maximum = Some(Key(op.value))
          case op: SetLeft => left = Some(op.value)
          case op: SetRight => right = Some(op.value)
          case op: Insert => 
            val key = Key(op.key)
            contents += (key -> Value(key, op.value, op.timestamp))
          case op: Delete => contents -= Key(op.value)
        }
      }

      new KeyValueObjectStoreState(idaEncodingIndex, minimum, maximum, left, right, contents)
    } catch {
      case t: Throwable => throw new KeyValueObjectEncodingError(t)
    }
  }
  
  def decodeUpdates(db: DataBuffer): List[KeyValueOperation] = {
    
    var ops = List[KeyValueOperation]()
    
    val bb = db.asReadOnlyBuffer()
      
    while (bb.remaining() > 0) {
      bb.getLong() // Update UUID mostSignificantBits
      bb.getLong() // Update UUID leastSignificantBits
      
      val updateSize = Varint.getUnsignedInt(bb)
      
      val updateEndPosition = bb.position + updateSize
        
      while (bb.position != updateEndPosition) {
        ops = KeyValueOperation.decode(bb) :: ops
      }
    }
    
    ops
  }
}