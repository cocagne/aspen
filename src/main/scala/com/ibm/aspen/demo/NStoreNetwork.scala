package com.ibm.aspen.demo

import com.ibm.aspen.core.network.StoreSideNetwork
import com.ibm.aspen.core.network.StoreSideReadHandler
import com.ibm.aspen.core.network.StoreSideAllocationHandler
import com.ibm.aspen.core.network.StoreSideTransactionHandler
import com.ibm.aspen.core.network.StoreSideReadMessageReceiver
import com.ibm.aspen.core.network.StoreSideAllocationMessageReceiver
import com.ibm.aspen.core.network.StoreSideTransactionMessageReceiver
import com.ibm.aspen.core.network.protocol.Message
import com.ibm.aspen.core.network.NetworkCodec
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.LocalUpdate
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.data_store.DataStoreID
import io.netty.channel.nio.NioEventLoopGroup
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.transaction.TxPrepareResponse
import com.ibm.aspen.core.transaction.TxAccept
import com.ibm.aspen.core.transaction.TxHeartbeat
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.allocation.AllocationStatusRequest
import com.ibm.aspen.core.allocation.AllocationStatusReply
import com.ibm.aspen.core.transaction.{Message => TransactionMessage}
import com.ibm.aspen.core.allocation.{Message => AllocationMessage}
import com.ibm.aspen.core.read.{Message => ReadMessage}
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.allocation.AllocateResponse
import com.ibm.aspen.core.read.Read
import com.ibm.aspen.core.read.ReadResponse
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.read
import com.ibm.aspen.core.allocation

class NStoreNetwork(val nodeName: String, val nnet: NettyNetwork) extends StoreSideNetwork 
   with StoreSideReadHandler with StoreSideAllocationHandler with StoreSideTransactionHandler {
  
  import NMessageEncoder._
  
  var or: Option[StoreSideReadMessageReceiver] = None
  var oa: Option[StoreSideAllocationMessageReceiver] = None
  var ot: Option[StoreSideTransactionMessageReceiver] = None
  
  def r = synchronized { or }
  def a = synchronized { oa }
  def t = synchronized { ot }
  
  val nodeConfig = nnet.config.nodes(nodeName)
  
  val connectionMgr = new NStoreConnectionManager(this, nnet.serverBossGroup, nnet.serverWorkerGroup, nodeConfig.endpoint.port)
  
  val stores = nnet.config.nodes.foldLeft(Map[DataStoreID, NClientConnection]()) { (m, n) =>
    val ep = n._2.endpoint
    val cnet = new NClientConnection(nnet.clientWorkerGroup, nodeConfig.uuid, ep.host, ep.port, (msg) => ())

    n._2.stores.foldLeft(m) { (m, s) =>
      m + (DataStoreID(nnet.config.pools(s.pool).uuid, s.store.asInstanceOf[Byte]) -> cnet)
    }
  }
  
  def receiveMessage(msg: Array[Byte]): Unit = synchronized {
    
    val bb = ByteBuffer.wrap(msg)
    val origLimit = bb.limit()
    val msgLen = bb.getInt()
    
    bb.limit(4+msgLen) // Limit to end of message
    
    // Must pass a read-only copy to the following method. It'll corrupt the rest of the buffer otherwise
    val p = Message.getRootAsMessage(bb.asReadOnlyBuffer())
    
    bb.limit(origLimit)
    bb.position(4 + msgLen) // reposition to encoded data
    
    if (p.prepare() != null) {
      //println("got prepare")
      val message = NetworkCodec.decode(p.prepare())
      
      val sb = message.txd.allReferencedObjectsSet.foldLeft(new StringBuilder)((sb, o) => sb.append(s" ${o.uuid}"))
      println(s"got prepare txid ${message.txd.transactionUUID} for objects: ${sb.toString()}")

      val updateContent = if (bb.remaining() == 0) None else {
        
        var localUpdates: List[LocalUpdate] = Nil
        
        // local update content is a series of <16-byte-uuid><4-byte-length><data>
        
        while (bb.remaining() != 0) {
          val msb = bb.getLong()
          val lsb = bb.getLong()
          val len = bb.getInt()
          val uuid = new UUID(msb, lsb)
        
          val slice = bb.asReadOnlyBuffer()
          slice.limit( slice.position + len )
          bb.position( bb.position + len )
          localUpdates = LocalUpdate(uuid, DataBuffer(slice)) :: localUpdates
        }
        
        Some(localUpdates)
      }
      
      t.foreach(receiver => receiver.receive(message, updateContent))
    } 
    else if (p.prepareResponse() != null) {
      println("got prepareResponse")
      val message = NetworkCodec.decode(p.prepareResponse())
      t.foreach(receiver => receiver.receive(message, None))
    }
    else if (p.accept() != null) {
      println("got accept")
      val message = NetworkCodec.decode(p.accept())
      t.foreach(receiver => receiver.receive(message, None))
    }
    else if (p.acceptResponse() != null) {
      println("got acceptResponse")
      val message = NetworkCodec.decode(p.acceptResponse())
      t.foreach(receiver => receiver.receive(message, None))
    }
    else if (p.resolved() != null) {
      val message = NetworkCodec.decode(p.resolved())
      println(s"got resolved for txid ${message.transactionUUID} committed = ${message.committed}")
      t.foreach(receiver => receiver.receive(message, None))
    }
    else if (p.finalized() != null) {
      println("got finalized")
      val message = NetworkCodec.decode(p.finalized())
      t.foreach(receiver => receiver.receive(message, None))
    }
    else if (p.heartbeat() != null) {
      val message = NetworkCodec.decode(p.heartbeat())
      t.foreach(receiver => receiver.receive(message, None))
    }
    else if (p.allocate() != null) {
      println(s"got allocate request. Receiver: $a")
      val message = NetworkCodec.decode(p.allocate())
      a.foreach(receiver => receiver.receive(message))
    }
    else if (p.allocateStatus() != null) {
      val message = NetworkCodec.decode(p.allocateStatus())
      a.foreach(receiver => receiver.receive(message))
    }
    else if (p.allocateStatusResponse() != null) {
      val message = NetworkCodec.decode(p.allocateStatusResponse())
      a.foreach(receiver => receiver.receive(message))
    }
    else if (p.read() != null) {
      val message = NetworkCodec.decode(p.read())
      r.foreach(receiver => receiver.receive(message))
    }
    else {
      println("Unknown Message!")
    }
  }
  
  // -----------------------------------------------------
  // StoreSideNetwork
  val readHandler: StoreSideReadHandler = this
  val allocationHandler: StoreSideAllocationHandler = this
  val transactionHandler: StoreSideTransactionHandler = this
  
  /** Called when a storage node begins hosting a data store */
  def registerHostedStore(storeId: DataStoreID): Unit = ()
  
  /** Called when a storage node stops hosting a data store */
  def unregisterHostedStore(storeId: DataStoreID): Unit = ()
  //-------------------------------------------------------
  
  def setReceiver(receiver: StoreSideReadMessageReceiver): Unit = synchronized { or = Some(receiver) }
  def setReceiver(receiver: StoreSideAllocationMessageReceiver): Unit = synchronized { oa = Some(receiver) }
  def setReceiver(receiver: StoreSideTransactionMessageReceiver): Unit = synchronized { ot = Some(receiver) }
  
  def send(message: TransactionMessage): Unit = {
    message match {
      case m: TxPrepareResponse => println(s"   prepare response disposition: ${m.disposition}")
      case _ => 
    }
    stores(message.to).send(encodeMessage(message))
  }
  
  def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit = {
    connectionMgr.sendMessageToClient(client.uuid, encodeMessage(acceptResponse))
  }
  
  def send(client: ClientID, resolved: TxResolved): Unit = {
    connectionMgr.sendMessageToClient(client.uuid, encodeMessage(resolved))
  }
  
  def send(client: ClientID, finalized: TxFinalized): Unit = {
    connectionMgr.sendMessageToClient(client.uuid, encodeMessage(finalized))
  }
  
  def sendPrepare(message: TxPrepare, updateContent: Option[List[LocalUpdate]] = None): Unit = {
    stores(message.to).send(encodeMessage(message, updateContent))
  }
  
  def send(client: ClientID, message: allocation.ClientMessage): Unit = {
    connectionMgr.sendMessageToClient(client.uuid, encodeMessage(message))
  }
  
  def send(message: AllocationStatusRequest): Unit = {
    stores(message.to).send(encodeMessage(message))
  }
  
  def send(message: AllocationStatusReply): Unit = {
    stores(message.to).send(encodeMessage(message))
  } 
  
  def send(client: ClientID, message: read.ReadResponse): Unit = {
    connectionMgr.sendMessageToClient(client.uuid, encodeMessage(message))
  }
}