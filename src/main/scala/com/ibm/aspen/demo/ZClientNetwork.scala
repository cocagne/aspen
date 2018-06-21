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
import org.zeromq.ZLoop
import org.zeromq.ZMQ.PollItem
import org.zeromq.ZMQ.Socket
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.LinkedBlockingQueue

class ZClientNetwork(override val clientId: ClientID, config: ConfigFile.Config) extends ClientSideNetwork 
    with ClientSideReadHandler with ClientSideAllocationHandler with ClientSideTransactionHandler {
  
  import ZMessageEncoder._
  
  val readHandler: ClientSideReadHandler = this
  val allocationHandler: ClientSideAllocationHandler = this
  val transactionHandler: ClientSideTransactionHandler = this
  
  var or: Option[ClientSideReadMessageReceiver] = None
  var oa: Option[ClientSideAllocationMessageReceiver] = None
  var ot: Option[ClientSideTransactionMessageReceiver] = None
  
  def r = synchronized { or }
  def a = synchronized { oa }
  def t = synchronized { ot }
  
  val sendQueue = new LinkedBlockingQueue[ () => Unit ]()
  
  val ctx = new ZContext()
  val reactor = new ZLoop(ctx)
  val handler = new ZLoop.IZLoopHandler {
    override def handle(loop: ZLoop, item: PollItem, arg: AnyRef): Int = {
      val msg = ZMsg.recvMsg(item.getSocket)
      
      if (msg != null) {
        
        val msgFrame = msg.pop().getData()
        
        val dataFrame = msg.pop() match {
          case null => None
          case zframe => Some(zframe.getData)
        }
        
        handleMessage(msgFrame, dataFrame)
        
        msg.destroy()
      }
      
      0
    }
  }
  
  val dealers = config.nodes.foldLeft(Map[DataStoreID, Socket]()) { (m, n) =>
    val dlr = ctx.createSocket(ZMQ.DEALER)
    dlr.setIdentity(com.ibm.aspen.util.uuid2byte(clientId.uuid))
    val ep = n._2.endpoint
    dlr.connect(s"tcp://${ep.host}:${ep.port}")
    reactor.addPoller(new PollItem(dlr, ZMQ.Poller.POLLIN), handler, ())
    n._2.stores.foldLeft(m) { (m, s) =>
      m + (DataStoreID(config.pools(s.pool).uuid, s.store.asInstanceOf[Byte]) -> dlr)
    }
  }
  
  val receiverThread = new Thread {
    setDaemon(true)
    
    override def run(): Unit = reactor.start()
  }
  receiverThread.start()
  
  val senderThread = new Thread {
    setDaemon(true)
    override def run(): Unit = {
      while (true)
        sendQueue.take()()
    }
  }
  senderThread.start()
  
  def handleMessage(msgFrame: Array[Byte], dataFrame: Option[Array[Byte]]): Unit = {

    val p = Message.getRootAsMessage(ByteBuffer.wrap(msgFrame))
  
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
      println(s"tx resolved ${message.transactionUUID} comitted = ${message.committed}")
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
  
  def enqueue(fn: => Unit): Unit = {
    sendQueue.add( () => fn )  
  }
  
  def psend(kind: String, zmsg: ZMsg): Unit = {
    var frames: List[Int] = Nil
    val i = zmsg.iterator()
    while (i.hasNext()) {
      val frame = i.next()
      frames = frame.getData.length :: frames
    }
    println(s"Sending $kind message with frame sizes: ${frames.reverse}. Frame count: ${frames.size}")
  }
  
  def send(toStore: DataStoreID, message: Allocate): Unit = enqueue {
    val zmsg = encodeMessage(None, message)
    psend("allocate", zmsg)
    zmsg.send(dealers(toStore))
  }
  
  def send(toStore: DataStoreID, message: Read): Unit  = enqueue {
    val zmsg = encodeMessage(None, message)
    //psend("read", zmsg)
    zmsg.send(dealers(toStore))
  }
  
  def send(message: TxPrepare, updateContent: List[LocalUpdate]): Unit = enqueue {
    val zmsg = encodePrepare(message, Some(updateContent))
    //psend("prepare", zmsg)
    val sb = message.txd.allReferencedObjectsSet.foldLeft(new StringBuilder)((sb, o) => sb.append(s" ${o.uuid}"))
    println(s"Sending prepare txid ${message.txd.transactionUUID} for objects: ${sb.toString()}")
    zmsg.send(dealers(message.to))
  }

}