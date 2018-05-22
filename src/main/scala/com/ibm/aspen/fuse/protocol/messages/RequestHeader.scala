package com.ibm.aspen.fuse.protocol.messages

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.OpCode

object RequestHeader {
  val HeaderSize = 40
}
  
class RequestHeader(bb: ByteBuffer) {
  	val len     = bb.getInt()
  	val opcode  = bb.getInt()
  	val unique  = bb.getLong()
  	val inode   = bb.getLong()
  	val uid     = bb.getInt()
  	val gid     = bb.getInt()
  	val pid     = bb.getInt()
  	val padding = bb.getInt()
  	
  	override def toString(): String = {
  	  val opname = OpCode.opcodeNames.getOrElse(opcode, "UNKNOWN")
  	  s"""|RequestHeader
        |  len:    $len
        |  opcode: $opcode
        |  opname: $opname
        |  unique: $unique
        |  inode:  $inode
        |  uid($uid), gid($gid), pid($pid)""".stripMargin
  	}
}
