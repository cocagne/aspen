package com.ibm.aspen.demo

import com.ibm.aspen.core.data_store.DataStoreID
import java.util.UUID

import com.ibm.aspen.core.network.ClientSideNetwork
import com.ibm.aspen.core.network.ClientSideReadHandler
import com.ibm.aspen.core.network.ClientSideAllocationHandler
import com.ibm.aspen.core.network.ClientSideTransactionHandler
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.network.ClientSideReadMessageReceiver
import com.ibm.aspen.core.network.ClientSideAllocationMessageReceiver
import com.ibm.aspen.core.network.ClientSideTransactionMessageReceiver
import java.nio.ByteBuffer

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.network.protocol.Message
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.read.{OpportunisticRebuild, Read, TransactionCompletionQuery}
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.transaction.LocalUpdate

class NClientNetwork(nnet: NettyNetwork) extends ClientSideNetwork 
    with ClientSideReadHandler with ClientSideAllocationHandler with ClientSideTransactionHandler {
  
  import NMessageEncoder._

  private[this] var osystem: Option[AspenSystem] = None

  def system: Option[AspenSystem] = synchronized(osystem)

  def setSystem(s: AspenSystem): Unit = synchronized(osystem = Some(s))
  
  override val clientId = ClientID(UUID.randomUUID())
  
  val onlineTracker = new OnlineTracker(nnet.config)
  
  println(s"CLIENT UUID: ${clientId.uuid}")
   
  val stores = nnet.config.nodes.foldLeft(Map[DataStoreID, NClientConnection]()) { (m, n) =>
    val ep = n._2.endpoint
    val cnet = new NClientConnection(nnet.clientWorkerGroup, clientId.uuid, n._2.uuid, ep.host, ep.port, receiveMessage, onlineTracker)

    n._2.stores.foldLeft(m) { (m, s) =>
      m + (DataStoreID(nnet.config.pools(s.pool).uuid, s.store.asInstanceOf[Byte]) -> cnet)
    }
  }
  
  val readHandler: ClientSideReadHandler = this
  val allocationHandler: ClientSideAllocationHandler = this
  val transactionHandler: ClientSideTransactionHandler = this
  
  var or: Option[ClientSideReadMessageReceiver] = None
  var oa: Option[ClientSideAllocationMessageReceiver] = None
  var ot: Option[ClientSideTransactionMessageReceiver] = None
  
  def r = synchronized { or }
  def a = synchronized { oa }
  def t = synchronized { ot }
  
  def receiveMessage(arr: Array[Byte]): Unit = {
    val bb = ByteBuffer.wrap(arr)
    val origLimit = bb.limit()
    val msgLen = bb.getInt()
    
    bb.limit(4+msgLen) // Limit to end of message
    
    val p = Message.getRootAsMessage(bb)
  
    if (p.readResponse() != null) {
      val message = NetworkCodec.decode(p.readResponse())
      r.foreach(receiver => receiver.receive(message))
    }
    else if (p.prepareResponse() != null) {
      val message = NetworkCodec.decode(p.prepareResponse())
      t.foreach(receiver => receiver.receive(message))
    }
    else if (p.acceptResponse() != null) {
      val message = NetworkCodec.decode(p.acceptResponse())
      t.foreach(receiver => receiver.receive(message))
    }
    else if (p.resolved() != null) {
      val message = NetworkCodec.decode(p.resolved())
      
      t.foreach(receiver => receiver.receive(message))
    }
    else if (p.finalized() != null) {
      val message = NetworkCodec.decode(p.finalized())
      t.foreach(receiver => receiver.receive(message))
    }
    else if (p.allocateResponse() != null) {
      val message = NetworkCodec.decode(p.allocateResponse())
      a.foreach(receiver => receiver.receive(message))
    }
  }
  
  def setReceiver(receiver: ClientSideReadMessageReceiver): Unit = synchronized { or = Some(receiver) }
  def setReceiver(receiver: ClientSideAllocationMessageReceiver): Unit = synchronized { oa = Some(receiver) }
  def setReceiver(receiver: ClientSideTransactionMessageReceiver): Unit = synchronized { ot = Some(receiver) }
  
  def send(toStore: DataStoreID, message: Allocate): Unit = {
    val msg = encodeMessage(message)
    
    stores(toStore).send(msg)
  }
  
  def send(message: Read): Unit  = {
    val msg = encodeMessage(message)
    
    stores(message.toStore).send(msg)
  }
  
  def send(message: OpportunisticRebuild): Unit  = {
    val msg = encodeMessage(message)
    
    stores(message.toStore).send(msg)
  }

  def send(message: TransactionCompletionQuery): Unit  = {
    val msg = encodeMessage(message)

    stores(message.toStore).send(msg)
  }
  
  def send(message: TxPrepare, updateContent: List[LocalUpdate]): Unit = {
    val msg = encodeMessage(message, Some(updateContent))
    
    val sb = message.txd.allReferencedObjectsSet.foldLeft(new StringBuilder)((sb, o) => sb.append(s" ${o.uuid}"))
    
    stores(message.to).send(msg)
  }
}