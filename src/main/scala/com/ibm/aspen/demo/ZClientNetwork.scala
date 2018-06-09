package com.ibm.aspen.demo

import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.network.ClientSideNetwork
import com.ibm.aspen.core.network.ClientSideTransactionHandler
import com.ibm.aspen.core.network.ClientSideAllocationHandler
import com.ibm.aspen.core.network.ClientSideReadHandler
import com.ibm.aspen.core.network.ClientSideReadMessageReceiver
import com.ibm.aspen.core.network.ClientSideAllocationMessageReceiver
import com.ibm.aspen.core.network.ClientSideTransactionMessageReceiver
import org.zeromq.ZContext
import org.zeromq.ZMQ
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.util.uuid2byte
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.read.Read
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.transaction.LocalUpdate
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.NetworkCodec
import org.zeromq.ZMsg
import com.ibm.aspen.core.network.protocol.Message

class ZClientNetwork(override val clientId: ClientID, config: ConfigFile.Config) extends ClientSideNetwork 
    with ClientSideReadHandler with ClientSideAllocationHandler with ClientSideTransactionHandler {
  
  import MessageEncoder._
  
  val readHandler: ClientSideReadHandler = this
  val allocationHandler: ClientSideAllocationHandler = this
  val transactionHandler: ClientSideTransactionHandler = this
  
  var or: Option[ClientSideReadMessageReceiver] = None
  var oa: Option[ClientSideAllocationMessageReceiver] = None
  var ot: Option[ClientSideTransactionMessageReceiver] = None
  
  def r = synchronized { or }
  def a = synchronized { oa }
  def t = synchronized { ot }
  
  val ctx = new ZContext()
  
  val rtr = ctx.createSocket(ZMQ.ROUTER)
  
  rtr.setIdentity(com.ibm.aspen.util.uuid2byte(clientId.uuid))
  
  val storeHosts = config.nodes.foldLeft(Map[DataStoreID, Array[Byte]]()) { (m, n) =>
    val addr = uuid2byte(n._2.uuid)
    rtr.connect(n._2.endpoint)
    n._2.stores.foldLeft(m) { (m, s) =>
      m + (DataStoreID(config.pools(s.pool).uuid, s.store.asInstanceOf[Byte]) -> addr)
    }
  }
  
  val receiverThread = new Thread {
    setDaemon(true)
    
    override def run(): Unit = {
      
      while (true) {
        val msg = ZMsg.recvMsg(rtr)
        
        msg.pop() // discard from address
        
        if (msg != null) {
          
          val p = Message.getRootAsMessage(ByteBuffer.wrap(msg.pop().getData()))
        
          if (p.readResponse() != null) {
            val message = NetworkCodec.decode(p.readResponse())
            r.foreach(receiver => receiver.receive(message))
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
        msg.destroy()
      }
    }
  }
  
  receiverThread.start()
  
  def setReceiver(receiver: ClientSideReadMessageReceiver): Unit = synchronized { or = Some(receiver) }
  def setReceiver(receiver: ClientSideAllocationMessageReceiver): Unit = synchronized { oa = Some(receiver) }
  def setReceiver(receiver: ClientSideTransactionMessageReceiver): Unit = synchronized { ot = Some(receiver) }
  
  def send(toStore: DataStoreID, message: Allocate): Unit = {
    val zmsg = encodeMessage(None, message)
    zmsg.addFirst(storeHosts(message.toStore))
    zmsg.send(rtr)
  }
  
  def send(toStore: DataStoreID, message: Read): Unit  = {
    val zmsg = encodeMessage(None, message)
    zmsg.addFirst(storeHosts(message.toStore))
    zmsg.send(rtr)
  }
  
  def send(message: TxPrepare, updateContent: List[LocalUpdate]): Unit = {
    val zmsg = encodePrepare(message, Some(updateContent))
    zmsg.addFirst(storeHosts(message.to))
    zmsg.send(rtr)
  }

}