package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Reply
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory
import java.nio.ByteBuffer

/*
 struct fuse_open_in {
	uint32_t	flags;
	uint32_t	unused;
};
 */
class OpenRequest(
    header:     RequestHeader,
    val flags:  Int,
    val unused: Int) extends Request(header) {
  
  override def toString(): String = s"""|OpenRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  flags($flags) unused($unused)""".stripMargin
}
    
object OpenRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): OpenRequest =  {

    val flags  = bb.getInt()
    val unused = bb.getInt()
    
    
    new OpenRequest(header, flags, unused)
  }
}
