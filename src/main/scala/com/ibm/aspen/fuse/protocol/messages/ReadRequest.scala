package com.ibm.aspen.fuse.protocol.messages

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory

/*struct fuse_read_in {
	uint64_t	fh;
	uint64_t	offset;
	uint32_t	size;
	uint32_t	read_flags;
	uint64_t	lock_owner;
	uint32_t	flags;
	uint32_t	padding;
};*/
class ReadRequest(
    header:            RequestHeader,
    val fileHandle:        Long,
    val offset:            Long,
    val size:              Long,
    val readFlags:         Int,
    val lockOwner:         Long,
    val flags:             Int,
    ) extends Request(header) {
  
  override def toString(): String = s"""|Read Request
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  fileHandle($fileHandle) offset($offset) size($size) readFlags($readFlags) lockOwner($lockOwner) flags($flags)""".stripMargin
}
    
object ReadRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): ReadRequest =  {

    val fileHandle = bb.getLong()
    val offset     = bb.getLong()
    val size       = uint32(bb.getInt())
    val readFlags  = bb.getInt()
    val (lockOwner, flags) = if (protocolVersion >= ProtocolVersion(7,9)) {
      val lo = bb.getLong()
      val flgs = bb.getInt()
      bb.getInt() // padding
      (lo, flgs)
    } else {
      (0L, 0)
    }
    
    new ReadRequest(header, fileHandle, offset, size, readFlags, lockOwner, flags)
  }
}
