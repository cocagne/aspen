package com.ibm.aspen.fuse.protocol.messages


import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Reply
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory
import java.nio.ByteBuffer

/*
 struct fuse_forget_in {
	uint64_t	nlookup;
};
 */
class ForgetRequest(
    header:     RequestHeader,
    val nlookup:  Long) extends Request(header) {
  
  override def toString(): String = s"""|ForgetRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  nlookup($nlookup)""".stripMargin
}
    
object ForgetRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): ForgetRequest =  {

    val nlookup  = bb.getLong()
    
    new ForgetRequest(header, nlookup)
  }
}