package com.ibm.aspen.fuse.protocol.messages

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory

/*
struct fuse_init_in {
	uint32_t	major;
	uint32_t	minor;
	uint32_t	max_readahead;
	uint32_t	flags;
};
 */
class InitRequest(
    header:            RequestHeader, // not part of actual message
    val requestLength: Int,           // not part of actual message
    val major:         Int,
    val minor:         Int,
    val max_readahead: Int,
    val flags:         Int) extends Request(header) {
  
  override def toString(): String = s"""|Init Request
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  major($major) minor($minor) max_readahead($max_readahead) flags($flags)""".stripMargin
}
    

object InitRequest extends RequestFactory {
  val RequestLength = 4*4
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): InitRequest =  {
    new InitRequest(header, RequestLength, bb.getInt(), bb.getInt(), bb.getInt(), bb.getInt())
  }
}
