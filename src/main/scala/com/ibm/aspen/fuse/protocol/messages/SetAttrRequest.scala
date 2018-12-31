package com.ibm.aspen.fuse.protocol.messages


import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory
import com.ibm.aspen.amorfs.Timespec

/*
struct fuse_setattr_in {
	uint32_t	valid;
	uint32_t	padding;
	uint64_t	fh;
	uint64_t	size;
	uint64_t	lock_owner;
	uint64_t	atime;
	uint64_t	mtime;
	uint64_t	ctime;
	uint32_t	atimensec;
	uint32_t	mtimensec;
	uint32_t	ctimensec;
	uint32_t	mode;
	uint32_t	unused4;
	uint32_t	uid;
	uint32_t	gid;
	uint32_t	unused5;
};
*/
class SetAttrRequest(
    header:         RequestHeader,
    val fileHandle: Option[Long],
    val size:       Option[Long],
    val lockOwner:  Option[Long],
    val atime:      Option[Timespec],
    val mtime:      Option[Timespec],
    val ctime:      Option[Timespec],
    val mode:       Option[Int],
    val newUID:     Option[Int],
    val newGID:     Option[Int]) extends Request(header) {
  
  override def toString(): String = s"""|SetAttrRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  fileHandle($fileHandle) size($size) lockOwner($lockOwner) atime($atime) mtime($mtime) ctime($ctime) mode(${mode.map(_.toOctalString)}) newUID($newUID) newGID($newGID)""".stripMargin
}
    
object SetAttrRequest extends RequestFactory {
  // Mask bits for the valid field
  val FATTR_MODE      = 1 << 0
  val FATTR_UID       = 1 << 1
  val FATTR_GID       = 1 << 2
  val FATTR_SIZE      = 1 << 3
  val FATTR_ATIME     = 1 << 4
  val FATTR_MTIME     = 1 << 5
  val FATTR_FH        = 1 << 6
  val FATTR_ATIME_NOW = 1 << 7
  val FATTR_MTIME_NOW = 1 << 8
  val FATTR_LOCKOWNER = 1 << 9
  val FATTR_CTIME     = 1 << 10
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): SetAttrRequest =  {

    val valid       = bb.getInt()
    bb.getInt()
    val fileHandle  = bb.getLong()
    val size        = bb.getLong()
    val lockOwner   = bb.getLong()
    val atime       = bb.getLong()
    val mtime       = bb.getLong()
    val ctime       = bb.getLong()
    val atimensec   = bb.getInt()
    val mtimensec   = bb.getInt()
    val ctimensec   = bb.getInt()
    val mode        = bb.getInt()
    bb.getInt()
    val newUID      = bb.getInt()
    val newGID      = bb.getInt()
    
    println(s"Setattr valid mask: ${valid.toBinaryString}")
    def ovalue[T](mask: Int, value: => T): Option[T] = if( (valid & mask) != 0 ) Some(value) else None
    
    new SetAttrRequest(header, 
        ovalue(FATTR_FH, fileHandle), 
        ovalue(FATTR_SIZE, size), 
        ovalue(FATTR_LOCKOWNER, lockOwner),
        ovalue(FATTR_ATIME, Timespec(atime, atimensec)),
        ovalue(FATTR_MTIME, Timespec(mtime, mtimensec)),
        ovalue(FATTR_CTIME, Timespec(ctime, ctimensec)),
        ovalue(FATTR_MODE, mode),
        ovalue(FATTR_UID, newUID),
        ovalue(FATTR_GID, newGID))
  }
}