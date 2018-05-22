package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.protocol.Reply
import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion

class DataReply(val buffers: List[ByteBuffer])(implicit val protocolVersion: ProtocolVersion) extends Reply {
 
  def staticReplyLength: Int = 0
  
  def write(bb:ByteBuffer): Unit = {}
  
  override private[protocol] def dataBuffers: List[ByteBuffer] = buffers
}
object DataReply {
  def apply(bb: ByteBuffer)(implicit protocolVersion: ProtocolVersion): DataReply = new DataReply(bb :: Nil)
}
