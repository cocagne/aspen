package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Reply
import java.nio.ByteBuffer

/*
 struct fuse_open_out {
	uint64_t	fh;
	uint32_t	open_flags;
	uint32_t	padding;
};
 */
class OpenReply(
    fileHandle: Long,
    openFlags: Int)(implicit val protocolVersion: ProtocolVersion) extends Reply {
 
  def staticReplyLength: Int = 8 + 2*4
  
  def write(bb:ByteBuffer): Unit = {
    bb.putLong(fileHandle)
    bb.putInt(openFlags)
    bb.putInt(0)
  }
}
