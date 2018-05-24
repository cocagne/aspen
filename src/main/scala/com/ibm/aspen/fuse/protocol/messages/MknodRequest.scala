package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Reply
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/*
 struct fuse_mknod_in {
	uint32_t	mode;
	uint32_t	rdev;
	uint32_t	umask;
	uint32_t	padding;
};
 */
class MknodRequest(
    header:    RequestHeader,
    val mode:  Int,
    val rdev:  Int,
    val umask: Int,
    val name:  String) extends Request(header) {
  
  override def toString(): String = s"""|MknodRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  mode($mode) rdev($rdev) umask($umask) name($name)""".stripMargin
}
    
object MknodRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): MknodRequest =  {

    val mode  = bb.getInt()
    val rdev  = bb.getInt()
    val umask = bb.getInt()
    bb.getInt()
    
    val bytes = new Array[Byte](bb.remaining())
    bb.get(bytes)
    
    new MknodRequest(header, mode, rdev, umask, new String(bytes, 0, bytes.length-1, StandardCharsets.UTF_8))
  }
}