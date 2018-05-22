package com.ibm.aspen.fuse.protocol.messages

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory

/*
struct fuse_getattr_in {
	uint32_t	getattr_flags;
	uint32_t	dummy;
	uint64_t	fh;
};
*/
class GetAttrRequest(
    header:                 RequestHeader,
    val getattr_flags:      Int,
    val dummy:              Int,
    val fileHandle:         Long) extends Request(header) {
  
  override def toString(): String = s"""|GetAttrRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  getattr_flags($getattr_flags) dummy($dummy) fileHandle($fileHandle)""".stripMargin
}
    
object GetAttrRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): GetAttrRequest =  {

    val getattr_flags = bb.getInt()
    val dummy         = bb.getInt()
    val fileHandle    = bb.getLong()
    
    new GetAttrRequest(header, getattr_flags, dummy, fileHandle)
  }
}
