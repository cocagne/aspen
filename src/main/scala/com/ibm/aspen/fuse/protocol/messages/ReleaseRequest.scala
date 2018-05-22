package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import java.nio.ByteBuffer

/*
 struct fuse_release_in {
	uint64_t	fh;
	uint32_t	flags;
	uint32_t	release_flags;
	uint64_t	lock_owner;
};
 */
class ReleaseRequest(
    header:           RequestHeader,
    val fileHandle:   Long,
    val flags:        Int,
    val releaseFlags: Int,
    val lockOwner:    Long) extends Request(header) {
  
  override def toString(): String = s"""|ReleaseRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  fileHandle($fileHandle) flags($flags) releaseFlags($releaseFlags) lockOwner($lockOwner)""".stripMargin
}
    
object ReleaseRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): ReleaseRequest =  {

    val fileHandle   = bb.getLong()
    val flags        = bb.getInt()
    val releaseFlags = bb.getInt()
    val lockOwner    = bb.getLong()
    
    new ReleaseRequest(header, fileHandle, flags, releaseFlags, lockOwner)
  }
}
