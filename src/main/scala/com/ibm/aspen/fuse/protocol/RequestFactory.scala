package com.ibm.aspen.fuse.protocol

import java.nio.ByteBuffer
import com.ibm.aspen.fuse.protocol.messages.RequestHeader

trait RequestFactory {
  
  def apply(protocolVersion: ProtocolVersion, header:RequestHeader, bb: ByteBuffer): Request
}
