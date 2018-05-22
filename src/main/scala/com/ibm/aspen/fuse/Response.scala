package com.ibm.aspen.fuse

import com.ibm.aspen.fuse.protocol.WireProtocol
import com.ibm.aspen.fuse.protocol.Reply

class Response[ReplyType <: Reply](private val wp: WireProtocol, val unique: Long) {
  def error(error: Int): Unit = wp.replyError(unique, error)
  
  def ok(reply: ReplyType): Unit = wp.replyOk(unique, reply)
}
