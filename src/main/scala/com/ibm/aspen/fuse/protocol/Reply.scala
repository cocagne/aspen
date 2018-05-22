package com.ibm.aspen.fuse.protocol

import java.nio.ByteBuffer

abstract class Reply {
  val protocolVersion: ProtocolVersion
   
  /** Includes only the static reply message size (not any following data). */
  def staticReplyLength: Int
  
  /** writes the reply message to the ByteBuffer (not any following data) */
  private[protocol] def write(bb:ByteBuffer): Unit
  
  /** List of data buffers to write after the reply message.
   *  
   *  The entries in this list will be reversed before they are written to the channel. This
   *  normal Scala list semantics may be used to prepend subsequent data buffers to the list.
   */
  private[protocol] def dataBuffers: List[ByteBuffer] = Nil
  
  /** Combined length of all data byte buffers or 0 if not data buffers are to be sent */
  def dataLength: Int = dataBuffers.foldLeft(0)((accum, bb) => accum + bb.remaining())
  
  /** Called after the write is complete. Any buffers allocated by ReplyBufferManager should be returned */
  def releaseBuffers(): Unit = {}
}
