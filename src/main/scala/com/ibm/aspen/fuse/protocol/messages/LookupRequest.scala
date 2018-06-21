package com.ibm.aspen.fuse.protocol.messages

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory
import java.nio.charset.StandardCharsets

/*
No structure. All bytes trailing the request header consist of the name of the directory entry to look up
*/
class LookupRequest(
    header:   RequestHeader,
    val name: String) extends Request(header) {
  
  override def toString(): String = s"""|LookupRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  name($name)""".stripMargin
}
    
object LookupRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): LookupRequest =  {

    val bytes = new Array[Byte](bb.remaining())
    bb.get(bytes)
    
    new LookupRequest(header, new String(bytes, 0, bytes.length-1, StandardCharsets.UTF_8))
  }
}
