package com.ibm.aspen.fuse.protocol

import java.nio.ByteBuffer

package object messages {
  def uint8(i: Byte): Short = (i.asInstanceOf[Short] & 0xFF).asInstanceOf[Short]
  def uint16(i: Int): Int  = i.asInstanceOf[Int] & 0xFFFF
  def uint32(i: Int): Long = i.asInstanceOf[Long] & 0xFFFFFFFL
  
  /** Returns the next aligned byte boundary beyond nbytes. 
   *  
   *  For example, if 10 is supplied to nbytes and 8 as the desired alignment, this method will
   *  return 16
   */
  def nextAlignmentBoundary(nbytes:Long, alignment:Int): Long = (nbytes+(alignment-1)) & ~(alignment-1)
  
  /** Returns the number of bytes that need to be added to nbytes to reach the desired alignment
   *  Example nbytes=10, alignment=8 will return 6  
   */
  def getPaddingToAlignment(nbytes:Long, alignment:Int): Int = {
    (nextAlignmentBoundary(nbytes, alignment) - nbytes).asInstanceOf[Int]
  }
  
  /** Inserts zero bytes in the byte buffer until the position reaches the desired alignment */
  def padToAlignment(bb:ByteBuffer, alignment:Int): Unit = {
    for ( i <- 0 until getPaddingToAlignment(bb.position, alignment) )
      bb.put(0.asInstanceOf[Byte])
  }
}
