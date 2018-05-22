package com.ibm.aspen.fuse.protocol.messages

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory

/** ReadDir requests are identical in format to ReadRequests so we'll implement this in terms of a ReadRequest */
class OpenDirRequest(open: OpenRequest) extends Request(open.header) {
  def flags = open.flags
  
  override def toString(): String = s"""|OpenDirRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  flags($flags)""".stripMargin
}
    
object OpenDirRequest extends RequestFactory {
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): OpenDirRequest =  {
    new OpenDirRequest(OpenRequest(protocolVersion, header, bb))
  }
}
