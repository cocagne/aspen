package com.ibm.aspen.demo

import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelHandler
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.ByteArrayDecoder
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.handler.codec.bytes.ByteArrayEncoder

object AspenChannelInitializer {
  val MaxFrameSize: Int = 1024 * 1024 * 101 // max is 100 MB
}

abstract class AspenChannelInitializer extends ChannelInitializer[SocketChannel] {
  import AspenChannelInitializer._
  
  def newChannel(): ChannelHandler
  
  override def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline()
    
    p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MaxFrameSize, 0, 4, 0, 4))
    p.addLast("bytesDecoder", new ByteArrayDecoder())
    
    p.addLast("frameEncoder", new LengthFieldPrepender(4))
    p.addLast("bytesEncoder", new ByteArrayEncoder())
    p.addLast("aspenChannel", newChannel())
  }
}