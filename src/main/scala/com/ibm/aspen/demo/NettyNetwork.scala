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

object NettyNetwork {
  val rnd = new java.util.Random
}

class NettyNetwork(val config: ConfigFile.Config) {

  import NettyNetwork._
  
  val serverBossGroup = new NioEventLoopGroup(1)
  val serverWorkerGroup = new NioEventLoopGroup
  val clientWorkerGroup = new NioEventLoopGroup
  
  val nodes = config.nodes.map( n => (n._2.uuid -> n._2) )
  val storeToNode = config.nodes.values.foldLeft(Map[DataStoreID, UUID]()) { (m, n) =>
    n.stores.foldLeft(m) { (sm, s) =>
      sm + (DataStoreID(config.pools(s.pool).uuid, s.store.asInstanceOf[Byte]) -> n.uuid)
    }
  }
  
  private[this] var onlineNodes = Set[UUID]()
  
  def setNodeOnline(nodeUUID: UUID): Unit = nodes.get(nodeUUID).foreach { store => 
    synchronized {
      println(s"Node Online: ${store.name}")
      onlineNodes += nodeUUID 
    }
  }
  
  def setNodeOffline(nodeUUID: UUID): Unit = nodes.get(nodeUUID).foreach { store => 
    synchronized {
      println(s"Node Offline: ${store.name}")
      onlineNodes -= nodeUUID 
    }
  }
  
  def isNodeOnline(nodeUUID: UUID): Boolean = synchronized { onlineNodes.contains(nodeUUID) }
  
  def isStoreOnline(storeId: DataStoreID): Boolean = isNodeOnline(storeToNode(storeId))
  
  def chooseDesignatedLeader(p: ObjectPointer): Byte = {
    var attempts = 0
    var online = false
    var idx = rnd.nextInt(p.ida.width).asInstanceOf[Byte]
    
    while(!online && attempts < p.ida.width) {
      attempts += 1
      online = isStoreOnline(DataStoreID(p.poolUUID, idx))
    }
    
    idx
  }
  
  def createStoreNetwork(nodeName: String) = new NStoreNetwork(nodeName, this)
  
  def createClientNetwork(): NClientNetwork = new NClientNetwork(this)
 
  def shutdown(): Unit = {
    serverBossGroup.shutdownGracefully()
    serverWorkerGroup.shutdownGracefully()
    clientWorkerGroup.shutdownGracefully()
  }
  
}