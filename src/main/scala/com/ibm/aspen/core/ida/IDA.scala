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
   *  Accepts a list of (Storage Pool Index, Option[Array[Byte]). All stores hosting segments are represented in the list.
   */
  def restore(segments: List[(Byte,Option[ByteBuffer])]): ByteBuffer
  
  /** Encodes the object and returns a mapping of StoragePoolIndex -> ByteBuffer
   */
  def encode(poolIndicies: List[Byte], objectContent: ByteBuffer): Map[Byte,ByteBuffer]
  
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
  
  def encode(poolIndicies: List[Byte], objectContent: ByteBuffer): Map[Byte,ByteBuffer] = poolIndicies.map( i => (i -> objectContent) ).toMap
}

case class ReedSolomon(width: Int, restoreThreshold: Int, writeThreshold: Int) 
  extends IDA {
  
  def consistentRestoreThreshold: Int = restoreThreshold
  
  def restore(segments: List[(Byte,Option[ByteBuffer])]): ByteBuffer = throw new IDAError("Read Solomon not yet supported")
  
  def encode(poolIndicies: List[Byte], objectContent: ByteBuffer): Map[Byte,ByteBuffer] = throw new IDAError("Read Solomon not yet supported")
}