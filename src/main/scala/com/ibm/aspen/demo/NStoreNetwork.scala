package com.ibm.aspen.demo

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network._
import com.ibm.aspen.core.network.protocol.Message
import com.ibm.aspen.core.{DataBuffer, allocation, read}
import com.ibm.aspen.core.transaction.{LocalUpdate, TxAcceptResponse, TxFinalized, TxPrepare, TxPrepareResponse, TxResolved, Message => TransactionMessage}

class NStoreNetwork(val nodeName: String, val nnet: NettyNetwork) extends StoreSideNetwork 
   with StoreSideReadHandler with StoreSideAllocationHandler with StoreSideTransactionHandler {
  
  import NMessageEncoder._

  private[this] var osystem: Option[AspenSystem] = None

  def system: Option[AspenSystem] = synchronized(osystem)

  def setSystem(s: AspenSystem): Unit = synchronized(osystem = Some(s))
  
  var or: Option[StoreSideReadMessageReceiver] = None
  var oa: Option[StoreSideAllocationMessageReceiver] = None
  var ot: Option[StoreSideTransactionMessageReceiver] = None
  
  def r = synchronized { or }
  def a = synchronized { oa }
  def t = synchronized { ot }
  
  val nodeConfig = nnet.config.nodes(nodeName)
  
  val connectionMgr = new NStoreConnectionManager(this, nodeConfig.endpoint.port)
  
  val onlineTracker = new OnlineTracker(nnet.config)
  
  val stores = nnet.config.nodes.foldLeft(Map[DataStoreID, NClientConnection]()) { (m, n) =>
    val ep = n._2.endpoint
    val cnet = new NClientConnection(nnet.clientWorkerGroup, nodeConfig.uuid, n._2.uuid, ep.host, ep.port, (msg) => (), onlineTracker)

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
    
    if (p.read() != null) {
      val message = NetworkCodec.decode(p.read())
      r.foreach(receiver => receiver.receive(message))
    }
    else if (p.prepare() != null) {
      //println("got prepare")
      val message = NetworkCodec.decode(p.prepare())
      
      val sb = message.txd.allReferencedObjectsSet.foldLeft(new StringBuilder)((sb, o) => sb.append(s" ${o.uuid}"))
      println(s"got prepare txid ${message.txd.transactionUUID} Leader ${message.txd.designatedLeaderUID} for objects: ${sb.toString()}")

      val updateContent = if (bb.remaining() == 0) None else {
        
        var localUpdates: List[LocalUpdate] = Nil
        
        // local update content is a series of <16-byte-uuid><4-byte-length><data>
        
        while (bb.remaining() != 0) {
          val msb = bb.getLong()
          val lsb = bb.getLong()
          val len = bb.getInt()
          val uuid = new UUID(msb, lsb)
        
          val slice = bb.asReadOnlyBuffer()
          slice.limit( slice.position() + len )
          bb.position( bb.position() + len )
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
    else if (p.committed() != null) {
      val message = NetworkCodec.decode(p.committed())
      println(s"got committed for txid ${message.transactionUUID}")
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
    else if (p.opportunisticRebuild() != null) {
      val message = NetworkCodec.decode(p.opportunisticRebuild())
      r.foreach(receiver => receiver.receive(message))
    }
    else if (p.transactionCompletionQuery() != null) {
      val message = NetworkCodec.decode(p.transactionCompletionQuery())
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
  
  def send(client: ClientID, prepareResponse: TxPrepareResponse): Unit = {
    connectionMgr.sendMessageToClient(client.uuid, encodeMessage(prepareResponse))  
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

  def send(client: ClientID, message: read.ReadResponse): Unit = {
    connectionMgr.sendMessageToClient(client.uuid, encodeMessage(message))
  }

  def send(client: ClientID, message: read.TransactionCompletionResponse): Unit = {
    connectionMgr.sendMessageToClient(client.uuid, encodeMessage(message))
  }
}