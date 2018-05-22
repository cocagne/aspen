package com.ibm.aspen.fuse

import com.ibm.aspen.fuse.protocol.IOBufferManager
import java.nio.ByteBuffer
import java.nio.ByteOrder

class DefaultBufferManager extends IOBufferManager {
  // Note that the largest buffer is 132 instead of 128. This provides an extra page for
  // capturing the message header in case the kernel decides to use the full 128Kb for
  // transferring data
  private var l132: List[ByteBuffer] = Nil
  private var  l64: List[ByteBuffer] = Nil
  private var  l32: List[ByteBuffer] = Nil
  private var  l16: List[ByteBuffer] = Nil
  private var   l4: List[ByteBuffer] = Nil
  
  private def rbuff(bb: ByteBuffer): Unit = {
    bb.clear()
    val cap = bb.capacity()
    
    if (cap == 132*1024) l132 = bb :: l132
    else if (cap == 64*1204) l64 = bb :: l64
    else if (cap == 32*1024) l32 = bb :: l32
    else if (cap == 16*1024) l16 = bb :: l16
    else if (cap == 4*1024) l4 = bb :: l4
  }
  
  def allocateDirectBuffer(minSizeInKB: Int): ByteBuffer = synchronized {
    val bb = if (minSizeInKB > 132) {
      ByteBuffer.allocateDirect(minSizeInKB*1024)
      
    } else if (minSizeInKB  > 64) { 
      if (l132.isEmpty) 
        ByteBuffer.allocateDirect(132*1024)
      else {
        val t = l132.head
        l132 = l132.tail
        t
      }
    }
    else if (minSizeInKB > 32) { 
      if (l64.isEmpty) 
        ByteBuffer.allocateDirect(64*1024)
      else {
        val t = l64.head
        l64 = l64.tail
        t
      }
    } 
    else if (minSizeInKB > 16) { 
      if (l32.isEmpty) 
        ByteBuffer.allocateDirect(32*1024)
      else {
        val t = l32.head
        l32 = l32.tail
        t
      }
    }
    else if (minSizeInKB > 4) { 
      if (l16.isEmpty) 
        ByteBuffer.allocateDirect(16*1024)
      else {
        val t = l16.head
        l16 = l16.tail
        t
      }
    } 
    else { 
      if (l4.isEmpty) 
        ByteBuffer.allocateDirect(4*1024)
      else {
        val t = l4.head
        l4 = l4.tail
        t
      }
    }
    
    bb.order(ByteOrder.nativeOrder())
    
    bb
  }
  
  def returnBuffer(bb: ByteBuffer): Unit = synchronized { rbuff(bb) }
    
  def returnBuffers(blist: List[ByteBuffer]): Unit = synchronized { blist.foreach(rbuff) }
}
