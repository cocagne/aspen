package com.ibm.aspen.util

import java.nio.ByteBuffer

/**
 *  Implements Variable-sized integer encoding according to the ProtocolBuffers description
 *  https://developers.google.com/protocol-buffers/docs/encoding?csw=1
 * 
 *  In particular, zig-zag encoding and even/odd mapping are used to effectively handle
 *  negative numbers.
 */
object Varint {
  
  class VarintEncodingError extends Exception
  
  def getSignedIntEncodingLength(v: Int): Int = {
    val bb = ByteBuffer.allocate(12)
    putSignedInt(bb, v)
    bb.position
  }
  def getUnignedIntEncodingLength(v: Int): Int = {
    val bb = ByteBuffer.allocate(12)
    putUnsignedInt(bb, v)
    bb.position
  }
  def getSignedLongEncodingLength(v: Int): Int = {
    val bb = ByteBuffer.allocate(12)
    putSignedLong(bb, v)
    bb.position
  }
  def getUnignedLongEncodingLength(v: Int): Int = {
    val bb = ByteBuffer.allocate(12)
    putUnsignedLong(bb, v)
    bb.position
  }
  
  def putSignedInt(bb: ByteBuffer, v: Int): Unit = putUnsignedInt(bb, (v << 1) ^ (v >> 31))
  def putSignedLong(bb: ByteBuffer, v: Long): Unit = putUnsignedLong(bb, (v << 1) ^ (v >> 63))

  def putUnsignedInt(bb: ByteBuffer, v: Int): Unit = {
    var x = v
    while((x & 0xFFFFF80) != 0L) {
      bb.put(((x & 0x7F) | 0x80).toByte)
      x >>>= 7
    }
    bb.put((x & 0x7F).toByte)
  }
  
  def putUnsignedLong(bb: ByteBuffer, v: Long): Unit = {
    var x = v
    while((x & 0xFFFFFFFFFFFFFF80L) != 0L) {
      bb.put(((x & 0x7F) | 0x80).toByte)
      x >>>= 7
    }
    bb.put((x & 0x7F).toByte)
  }

  def getSignedInt(bb: ByteBuffer): Int = {
    val unsigned = getUnsignedInt(bb)
    
    val tmp = (((unsigned << 31) >> 31) ^ unsigned) >> 1
    
    tmp ^ (unsigned & (1 << 31))
  }

  def getUnsignedInt(bb: ByteBuffer): Int = {
    var i = 0
    var v = 0
    var read = 0
    do {
      read = bb.get
      v |= (read & 0x7F) << i
      i += 7
      if (i > 35) throw new VarintEncodingError
    } while((read & 0x80) != 0)
    v
  }

  def getSignedLong(bb: ByteBuffer): Long = {
    val unsigned = getUnsignedLong(bb)
    
    val tmp = (((unsigned << 63) >> 63) ^ unsigned) >> 1
    
    tmp ^ (unsigned & (1L << 63))
  }

  def getUnsignedLong(bb: ByteBuffer): Long = {
    var i = 0
    var v = 0L
    var read = 0L
    do {
      read = bb.get
      v |= (read & 0x7F) << i
      i += 7
      if (i > 70) throw new VarintEncodingError
    } while((read & 0x80L) != 0)
    v
  }
}