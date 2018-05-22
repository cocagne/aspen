package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.DirEntry
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Reply
import java.nio.ByteBuffer

/*
 struct fuse_entry_out {
	uint64_t	nodeid;		/* Inode ID */
	uint64_t	generation;	/* Inode generation: nodeid:gen must
					   be unique for the fs's lifetime */
	uint64_t	entry_valid;	/* Cache timeout for the name */
	uint64_t	attr_valid;	/* Cache timeout for the attributes */
	uint32_t	entry_valid_nsec;
	uint32_t	attr_valid_nsec;
	struct fuse_attr attr;
};

fuse_attr is handled by GetAttrReply
 */
class DirEntryReply(entry: DirEntry)(implicit val protocolVersion: ProtocolVersion) extends Reply {
 
  def staticReplyLength: Int = if (protocolVersion >= ProtocolVersion(7,9)) 
    8*4 + 4*2 + 8*6 + 4*10 
  else 
    120
  
  def write(bb:ByteBuffer): Unit = {
    bb.putLong(entry.inode)
    bb.putLong(entry.generation)
    bb.putLong(entry.entryTimeout)
    bb.putLong(entry.attrTimeout)
    bb.putInt(entry.entryTimeoutNsec)
    bb.putInt(entry.attrTimeoutNsec)
    GetAttrReply.writeStat(bb, entry.stat)
  }
}
