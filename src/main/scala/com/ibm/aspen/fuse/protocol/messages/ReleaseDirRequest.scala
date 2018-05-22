package com.ibm.aspen.fuse.protocol.messages

import com.ibm.aspen.fuse.protocol.Request
import com.ibm.aspen.fuse.protocol.RequestFactory
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import java.nio.ByteBuffer

class ReleaseDirRequest(release: ReleaseRequest) extends Request(release.header) {
  
  def fileHandle   = release.fileHandle
  def flags        = release.flags
  def releaseFlags = release.flags
  def lockOwner    = release.lockOwner
  
  override def toString(): String = s"""|ReleaseDirRequest
  |  len(${header.len}) opcode(${header.opcode}) unique($unique) inode($inode) uid($uid), gid($gid), pid($pid)
  	|  fileHandle($fileHandle) flags($flags) releaseFlags($releaseFlags) lockOwner($lockOwner)""".stripMargin
}
    
object ReleaseDirRequest extends RequestFactory {
  
  override def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb:ByteBuffer): ReleaseDirRequest =  {
    new ReleaseDirRequest(ReleaseRequest(protocolVersion, header, bb))
  }
}
