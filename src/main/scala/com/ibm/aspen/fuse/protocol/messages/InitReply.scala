package com.ibm.aspen.fuse.protocol.messages

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.ProtocolVersion
import com.ibm.aspen.fuse.protocol.Reply

case class InitReply(
  val protocolVersion:      ProtocolVersion,
  	val major:                Int,
  	val minor:                Int,
  	val	max_readahead:        Int,
  	val	flags:                Int,
  	val	max_background:       Short,
  	val	congestion_threshold: Short,
  	val	max_write:            Int,
  	val	time_gran:            Int
  	//uint32_t	unused[9]: Int
  	) extends Reply {
  
  def staticReplyLength: Int = if (protocolVersion < ProtocolVersion(7,23)) 24 else 64
  
  	def write(bb:ByteBuffer) = {
  	  bb.putInt(major)
  	  bb.putInt(minor)
  	  bb.putInt(max_readahead)
  	  bb.putInt(flags)
  	  bb.putShort(max_background)
  	  bb.putShort(congestion_threshold)
  	  bb.putInt(max_write)
  	  
  	  if (protocolVersion >= ProtocolVersion(7,23)) {
  	    bb.putInt(time_gran)
  	    
  	    for (i <- 0 until 9)
  	      bb.putInt(0)
  	  } 
  	}
}
