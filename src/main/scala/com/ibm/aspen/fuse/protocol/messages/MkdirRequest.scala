package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Reply
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/*
 struct fuse_mkdir_in {
	uint32_t	mode;
	uint32_t	umask;
};
 */
class MkdirRequest(
    header:    RequestHeader,
    val mode:  Int,
    val umask: Int,
    val name:  String) extends Request(header) {
  
  override def toString(): String = s"""|MkdirRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  mode($mode) umask($umask) name($name)""".stripMargin
}
    
object MkdirRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): MkdirRequest =  {

    val mode  = bb.getInt()
    val umask = bb.getInt()
    
    val bytes = new Array[Byte](bb.remaining())
    bb.get(bytes)
    
    new MkdirRequest(header, mode, umask, new String(bytes, 0, bytes.length-1, StandardCharsets.UTF_8))
  }
}