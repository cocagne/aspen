package com.ibm.aspen.fuse.protocol.messages

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory
import java.nio.charset.StandardCharsets

/*
 struct fuse_rename_in {
	uint64_t	newdir;
};

Following the structure are two null-terminated strings. First the old name, then the new name
*/
class RenameRequest(
    header:   RequestHeader,
    val newDirInode: Long,
    val oldName: String,
    val newName: String) extends Request(header) {
  
  override def toString(): String = s"""|RenameRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  newDir($newDirInode) oldName($oldName) newName($newName)""".stripMargin
}
    
object RenameRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): RenameRequest =  {
    
    val newDirInode = bb.getLong()
    
    val bytes = new Array[Byte](bb.remaining())
    bb.get(bytes)
    
    def getNullIndex(i: Int): Int = if (bytes(i) == 0) i else getNullIndex(i+1)
    
    val firstNullIndex = getNullIndex(0)
    println(s"Total byte len ${bytes.length} first null index $firstNullIndex")
    val oldName = new String(bytes, 0, firstNullIndex, StandardCharsets.UTF_8)
    println(s"oldName $oldName")
    println(s"Byte array: ${java.util.Arrays.toString(bytes)}")
    val newName = new String(bytes, firstNullIndex+1, bytes.length-firstNullIndex-2, StandardCharsets.UTF_8)
    println(s"newName $newName")
    
    new RenameRequest(header, newDirInode, oldName, newName)
  }
}