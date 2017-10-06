package com.ibm.aspen.core.ida

import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.read.IDAError
import java.nio.ByteBuffer

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
   *  Accepts a list of (EncodingIndex, Option[Array[Byte]).
   *  Where the encoding index is the index of this data within the corresponding encode() call 
   */
  def restore(segments: List[(Byte,Option[ByteBuffer])]): ByteBuffer
  
  /** Encodes the object into an array of ByteBuffers
   *  
   *  Note that the indices of this array are known as the EncodingIndex and are significant to the
   *  corresponding decode operation. The correct index in this array must be used during the decoding
   *  process.
   */
  def encode(objectContent: ByteBuffer): Array[ByteBuffer]
  
  def failureTolerance: Int = width - writeThreshold
  
  def compare(that: IDA) = failureTolerance - that.failureTolerance
}

case class Replication(width: Int, writeThreshold: Int) extends IDA {
  
  def restoreThreshold: Int = 1
  
  def consistentRestoreThreshold: Int = width / 2 + 1
  
  def restore(segments: List[(Byte,Option[ByteBuffer])]): ByteBuffer = segments.find(t => t._2 match {
    case None => false
    case Some(_) => true
  }) match {
    case None => throw new IDAError("No stores returned data")
    case Some(t) => t._2.get
  }
  
  def encode(objectContent: ByteBuffer): Array[ByteBuffer] = {
    val arr = new Array[ByteBuffer](width)
    for (i <- 0 until width)
      arr(i) = objectContent.asReadOnlyBuffer()
    arr
  }
}

case class ReedSolomon(width: Int, restoreThreshold: Int, writeThreshold: Int) 
  extends IDA {
  
  def consistentRestoreThreshold: Int = restoreThreshold
  
  def restore(segments: List[(Byte,Option[ByteBuffer])]): ByteBuffer = throw new IDAError("Read Solomon not yet supported")
  
  def encode(objectContent: ByteBuffer): Array[ByteBuffer] = throw new IDAError("Read Solomon not yet supported")
}