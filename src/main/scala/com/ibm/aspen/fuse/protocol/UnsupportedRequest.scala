package com.ibm.aspen.fuse.protocol

import com.ibm.aspen.fuse.protocol.messages.RequestHeader

/** Represents a request for which we have not yet added support */
class UnsupportedRequest(header:RequestHeader) extends Request(header) {
  def requestLength: Int = header.len - RequestHeader.HeaderSize
}
