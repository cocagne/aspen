package com.ibm.aspen.fuse

import java.nio.channels.SocketChannel
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer


object RemoteFuse {
  def connect(host: String, port: Int, fuseMountOptions: Option[String]): SocketChannel = {
    val channel = SocketChannel.open( new InetSocketAddress(host, port) )
    
    // Send length-prefixed fuse mount options string
    val moarr = fuseMountOptions.getOrElse("").getBytes(StandardCharsets.UTF_8)
    val bb = ByteBuffer.allocate(4+moarr.length)
    bb.putInt(moarr.length)
    bb.put(moarr)
    bb.flip()
    while (bb.hasRemaining())
      channel.write(bb)
      
    channel
  }
}
