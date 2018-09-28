package com.ibm.aspen.demo

import io.netty.channel.nio.NioEventLoopGroup
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.ChannelOption
import io.netty.handler.logging.LoggingHandler
import io.netty.handler.logging.LogLevel
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.bytes.ByteArrayEncoder
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.handler.codec.bytes.ByteArrayDecoder
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.bootstrap.Bootstrap
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import java.util.UUID

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.{AspenSystem, StorageHost}

import scala.concurrent.ExecutionContext
import scala.concurrent.Future



class NettyNetwork(val config: ConfigFile.Config) {

  val serverBossGroup = new NioEventLoopGroup(1)
  val serverWorkerGroup = new NioEventLoopGroup
  val clientWorkerGroup = new NioEventLoopGroup

  private var stores: List[NStoreNetwork] = Nil
  private var oclient: Option[NClientNetwork] = None

  def setSystem(system: AspenSystem) = {
    stores.foreach(_.setSystem(system))
    oclient.foreach(_.setSystem(system))
  }
  
  def createStoreNetwork(nodeName: String) = synchronized {
    val s = new NStoreNetwork(nodeName, this)
    stores = s :: stores
    s
  }
  
  def createClientNetwork(): NClientNetwork = synchronized {
    val c = new NClientNetwork(this)
    oclient = Some(c)
    c
  }
 
  def shutdown(): Unit = {
    serverBossGroup.shutdownGracefully()
    serverWorkerGroup.shutdownGracefully()
    clientWorkerGroup.shutdownGracefully()
  }
  
}