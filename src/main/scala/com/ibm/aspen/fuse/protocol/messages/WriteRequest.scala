package com.ibm.aspen.fuse.protocol.messages

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory

/*struct fuse_write_in {
	uint64_t	fh;
	uint64_t	offset;
	uint32_t	size;
	uint32_t	write_flags;
	uint64_t	lock_owner;
	uint32_t	flags;
	uint32_t	padding;
};*/
class WriteRequest(
    header:            RequestHeader,
    val fileHandle:        Long,
    val offset:            Long,
    val size:              Long,
    val writeFlags:         Int,
    val lockOwner:         Long,
    val flags:             Int,
    val data:              ByteBuffer
    ) extends Request(header) {
  
  override def toString(): String = s"""|WriteRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  fileHandle($fileHandle) offset($offset) size($size) writeFlags($writeFlags) lockOwner($lockOwner) flags($flags) dataSize(${data.remaining()})""".stripMargin
}
    
object WriteRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): WriteRequest =  {
    val fileHandle = bb.getLong()
    val offset     = bb.getLong()
    val size       = uint32(bb.getInt())
    val writeFlags  = bb.getInt()
    val (lockOwner, flags) = if (protocolVersion >= ProtocolVersion(7,9)) {
      val lo = bb.getLong()
      val flgs = bb.getInt()
      bb.getInt() // padding
      (lo, flgs)
    } else {
      (0L, 0)
    }
    
    val data = bb.slice().asReadOnlyBuffer()
    
    new WriteRequest(header, fileHandle, offset, size, writeFlags, lockOwner, flags, data)
  }
}