package com.ibm.aspen.core.ida

import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.read.IDAError
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer

sealed abstract class IDA extends Ordered[IDA] {
  
  /** Number of slices/replicas */
  def width: Int
  
  /** Minimum number of slices needed to restore the object to a readable state.
   * This value may be less than consistentRestoreThreshold.   
   */
  def restoreThreshold: Int
  
  /** Minimum number of slices/replicas that must agree on the current object revision in order to guarantee
   * consistency.
   */
  def consistentRestoreThreshold: Int
  
  /** Minimum number of slices/replicas that must be successfully written for a successful update transaction */
  def writeThreshold: Int
  
  /** Restores the data or throws an Exception if the restore operation fails. 
   *  Accepts a list of (EncodingIndex, Option[DataBuffer]).
   *  Where the encoding index is the index of this data within the corresponding encode() call 
   */
  def restore(segments: List[(Byte,Option[DataBuffer])]): DataBuffer
  
  /** Encodes the object into an array of ByteBuffers
   *  
   *  Note that the indices of this array are known as the EncodingIndex and are significant to the
   *  corresponding decode operation. The correct index in this array must be used during the decoding
   *  process.
   */
  def encode(objectContent: DataBuffer): Array[DataBuffer]
  
  def failureTolerance: Int = width - writeThreshold
  
  def compare(that: IDA) = failureTolerance - that.failureTolerance
}

case class Replication(width: Int, writeThreshold: Int) extends IDA {
  
  def restoreThreshold: Int = 1
  
  def consistentRestoreThreshold: Int = width / 2 + 1
  
  def restore(segments: List[(Byte,Option[DataBuffer])]): DataBuffer = segments.find(t => t._2 match {
    case None => false
    case Some(_) => true
  }) match {
    case None => throw new IDAError("No stores returned data")
    case Some(t) => t._2.get
  }
  
  def encode(objectContent: DataBuffer): Array[DataBuffer] = {
    val arr = new Array[DataBuffer](width)
    for (i <- 0 until width)
      arr(i) = objectContent
    arr
  }
}

case class ReedSolomon(width: Int, restoreThreshold: Int, writeThreshold: Int) 
  extends IDA {
  
  def consistentRestoreThreshold: Int = restoreThreshold
  
  def restore(segments: List[(Byte,Option[DataBuffer])]): DataBuffer = throw new IDAError("Read Solomon not yet supported")
  
  def encode(objectContent: DataBuffer): Array[DataBuffer] = throw new IDAError("Read Solomon not yet supported")
}