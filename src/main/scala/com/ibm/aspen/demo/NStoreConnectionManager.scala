package com.ibm.aspen.demo

import java.util.UUID

import com.ibm.aspen.util.byte2uuid
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelOption}
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.logging.log4j.scala.Logging

class NStoreConnectionManager(
    val storeNetwork: NStoreNetwork, 
    val port: Int) extends Logging {
 
  private val serverBoot = new ServerBootstrap
  
  private[this] var clientConnections = Map[UUID, ChannelHandlerContext]()
  
  class StoreChannelHandler(snet: NStoreNetwork) extends ChannelInboundHandlerAdapter {
    
    // UUID of the client. Set in first message from client
    var oclientUUID: Option[UUID] = None
    
    override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = synchronized {
      msg match {
        case arr: Array[Byte] => oclientUUID match {
          case None => 
            if (arr.length == 16) {
              val clientUUID = byte2uuid(arr)
              oclientUUID = Some(clientUUID)
              updateClientConnetion(clientUUID, Some(ctx))
            } else
              logger.error(s"RECEIVED UNEXPECTED INITIAL MESSAGE OF SIZE: ${arr.length}")
              
          case Some(_) => storeNetwork.receiveMessage(arr)
        }
        
        case x => logger.error(s"RECEIVED UNEXPECTED MESSAGE TYPE: $x")
      }
    }

    override def channelInactive(ctx: ChannelHandlerContext): Unit = synchronized {
      oclientUUID.foreach { clientUUID => 
        updateClientConnetion(clientUUID, None)
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      // Close the connection when an exception is raised.
      cause.printStackTrace();
      ctx.close();
    }
  }
  
  def updateClientConnetion(clientUUID: UUID, octx: Option[ChannelHandlerContext]): Unit = synchronized {
    octx match {
      case None => clientConnections -= clientUUID
      case Some(ctx) => clientConnections += (clientUUID -> ctx)
    }
  }
  
  def sendMessageToClient(clientUUID: UUID, msg: Array[Byte]): Unit = synchronized {
    clientConnections.get(clientUUID).foreach(_.writeAndFlush(msg))
  }
  
  serverBoot.group(storeNetwork.nnet.serverBossGroup, storeNetwork.nnet.serverWorkerGroup)
            .channel(classOf[NioServerSocketChannel])
            .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 100)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new AspenChannelInitializer {
               def newChannel() = new StoreChannelHandler(storeNetwork)
             })

  // Start the server.
  serverBoot.bind(port).sync()
}