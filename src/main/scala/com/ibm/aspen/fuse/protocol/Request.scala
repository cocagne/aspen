package com.ibm.aspen.fuse.protocol

import com.ibm.aspen.fuse.protocol.messages.RequestHeader

/** Base class for all Kernel -> Fuse Daemon requests */
abstract class Request(val header: RequestHeader) {  
  def unique: Long = header.unique
  def inode:  Long = header.inode
  def uid:    Int  = header.uid
  def gid:    Int  = header.gid
  def pid:    Int  = header.pid
}
