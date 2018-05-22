package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.protocol.ProtocolVersion
import java.nio.ByteBuffer
import com.ibm.aspen.fuse.Stat
import com.ibm.aspen.fuse.protocol.Reply

/*
struct fuse_attr_out {
	uint64_t	attr_valid;	/* Cache timeout for the attributes */
	uint32_t	attr_valid_nsec;
	uint32_t	dummy;
	struct fuse_attr attr;
};

struct fuse_attr {
	uint64_t	ino;
	uint64_t	size;
	uint64_t	blocks;
	uint64_t	atime;
	uint64_t	mtime;
	uint64_t	ctime;
	uint32_t	atimensec;
	uint32_t	mtimensec;
	uint32_t	ctimensec;
	uint32_t	mode;
	uint32_t	nlink;
	uint32_t	uid;
	uint32_t	gid;
	uint32_t	rdev;
	uint32_t	blksize; // Added in 7.9
	uint32_t	padding; // Added in 7.9
};
*/
object GetAttrReply {
  def writeStat(bb:ByteBuffer, stat: Stat)(implicit protocolVersion: ProtocolVersion): Unit = {
    bb.putLong(stat.inode)
    bb.putLong(stat.size)
    bb.putLong(stat.blocks)
    bb.putLong(stat.atime)
    bb.putLong(stat.mtime)
    bb.putLong(stat.ctime)
    bb.putInt(stat.atimensec)
    bb.putInt(stat.mtimensec)
    bb.putInt(stat.ctimensec)
    bb.putInt(stat.mode)
    bb.putInt(stat.nlink)
    bb.putInt(stat.uid)
    bb.putInt(stat.gid)
    bb.putInt(stat.rdev)
    if (protocolVersion >= ProtocolVersion(7,9)) {
      bb.putInt(stat.blksize)
      bb.putInt(0)
    }
  }
}

class GetAttrReply(
    attr_valid: Long,
    attr_valid_nsec: Int,
    stat: Stat)(implicit val protocolVersion: ProtocolVersion) extends Reply {
 
  import GetAttrReply._
  
  def staticReplyLength: Int = if (protocolVersion >= ProtocolVersion(7,9)) 
    (8 + 4*2) + (8*6 + 4*10)
  else
    96    
  
  def write(bb:ByteBuffer): Unit = {
    bb.putLong(attr_valid)
    bb.putInt(attr_valid_nsec)
    bb.putInt(0)
    writeStat(bb, stat)
  }
}
