package com.ibm.aspen.fuse.protocol.messages


import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Reply
import java.nio.ByteBuffer

/*
 struct fuse_write_out {
	uint32_t	size;
	uint32_t	padding;
};
 */
class WriteReply(size: Long)(implicit val protocolVersion: ProtocolVersion) extends Reply {
 
  def staticReplyLength: Int = 4 + 4
  
  def write(bb:ByteBuffer): Unit = {
    bb.putInt(size.asInstanceOf[Int])
    bb.putInt(0)
  }
}