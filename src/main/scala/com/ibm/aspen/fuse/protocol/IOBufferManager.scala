package com.ibm.aspen.fuse.protocol

import java.nio.ByteBuffer

/** Manages a pool of direct buffers for use in Reply messages.
 *  
 *  The Byte buffers allocated by this manager must be returned to it after the response is
 *  written out to the kernel by the wire protocol
 */
trait IOBufferManager {
  
  /** Allocates a Direct ByteBuffer using the platform's native byte order
   *  . 
   *  @param minSizeInKB Minimum size in Kilobytes of the buffer to be allocated
   *  @return [[java.nio.ByteBuffer]] 
   */
  def allocateDirectBuffer(minSizeInKB: Int): ByteBuffer
  
  def returnBuffer(bb: ByteBuffer): Unit
  
  def returnBuffers(blist: List[ByteBuffer]): Unit
  
}
