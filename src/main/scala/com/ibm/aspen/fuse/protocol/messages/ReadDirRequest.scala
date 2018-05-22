package com.ibm.aspen.fuse.protocol.messages

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory

/** ReadDir requests are identical in format to ReadRequests so we'll implement this in terms of a ReadRequest */
class ReadDirRequest(read: ReadRequest) extends Request(read.header) {
  def fileHandle: Long = read.fileHandle
  def offset:     Long = read.offset
  def size:       Long = read.size
  def readFlags:  Int  = read.flags
  def lockOwner:  Long = read.lockOwner
  def flags:      Int  = read.flags
  
  override def toString(): String = s"""|ReadDir Request
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  fileHandle($fileHandle) offset($offset) size($size) readFlags($readFlags) lockOwner($lockOwner) flags($flags)""".stripMargin
}
    
object ReadDirRequest extends RequestFactory {
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): ReadDirRequest =  {
    new ReadDirRequest(ReadRequest(protocolVersion, header, bb))
  }
}
