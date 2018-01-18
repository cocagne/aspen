package com.ibm.aspen.core.objects.keyvalue

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.util.Varint

/** Represents the decoded object state from a single DataStore.
 */
class KeyValueObjectStoreState(
    val ida: IDA,
    val idaEncodingIndex: Byte,
    val minimum: Option[Array[Byte]],
    val maximum: Option[Array[Byte]],
    val idaEncodedLeft: Option[Array[Byte]],
    val idaEncodedRight: Option[Array[Byte]],
    val idaEncodedContents: Map[Array[Byte], KVState]) 
    
object KeyValueObjectStoreState {
  
  def apply(ida: IDA, idaEncodingIndex: Byte, db: DataBuffer): KeyValueObjectStoreState = {
    try {
      var minimum: Option[Array[Byte]] = None
      var maximum: Option[Array[Byte]] = None
      var left: Option[Array[Byte]] = None
      var right: Option[Array[Byte]] = None
      var contents: Map[Array[Byte], KVState] = Map()
     
      val bb = db.asReadOnlyBuffer()
      
      while (bb.remaining() > 0) {
        bb.getLong() // Update UUID mostSignificantBits
        bb.getLong() // Update UUID leastSignificantBits
        val updateSize = Varint.getUnsignedInt(bb)
        
        val updateEndPosition = bb.position + updateSize
          
        while (bb.position != updateEndPosition) {
          KeyValueOperation.decode(bb) match {
            case op: SetMin => minimum = Some(op.value)
            case op: SetMax => maximum = Some(op.value)
            case op: SetLeft => maximum = Some(op.value)
            case op: SetRight => maximum = Some(op.value)
            case op: Insert => contents += (op.key -> KVState(op.key, op.value, op.timestamp))
            case op: Delete => contents -= op.value
          }
        }
        
      }
      
      new KeyValueObjectStoreState(ida, idaEncodingIndex, minimum, maximum, left, right, contents)
    } catch {
      case t: Throwable => throw new KeyValueObjectEncodingError(t)
    }
  }
}