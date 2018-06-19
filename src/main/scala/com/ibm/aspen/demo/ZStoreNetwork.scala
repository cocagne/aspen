package com.ibm.aspen.demo

import org.zeromq._
import org.zeromq.ZMQ.Poller
import org.zeromq.ZMQ.Socket

import com.ibm.aspen.core.network.StoreSideNetwork
import com.ibm.aspen.core.network.StoreSideReadHandler
import com.ibm.aspen.core.network.StoreSideAllocationHandler
import com.ibm.aspen.core.network.StoreSideTransactionHandler
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.StoreSideReadMessageReceiver
import com.ibm.aspen.core.network.StoreSideAllocationMessageReceiver
import com.ibm.aspen.core.network.StoreSideTransactionMessageReceiver
import com.ibm.aspen.core.network.protocol.Message
import java.nio.ByteBuffer
import com.ibm.aspen.core.network.NetworkCodec
import com.ibm.aspen.core.read
import com.ibm.aspen.core.allocation
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.allocation.AllocationStatusRequest
import com.ibm.aspen.core.allocation.AllocationStatusReply
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.aspen.core.transaction.TxPrepareResponse
import com.ibm.aspen.core.transaction.TxAccept
import com.ibm.aspen.core.transaction.TxHeartbeat
import com.ibm.aspen.util.uuid2byte
import com.ibm.aspen.core.transaction.{Message => TransactionMessage}
import com.ibm.aspen.core.allocation.{Message => AllocationMessage}
import com.ibm.aspen.core.read.{Message => ReadMessage}
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.allocation.AllocateResponse
import com.ibm.aspen.core.read.Read
import com.ibm.aspen.core.read.ReadResponse
import org.zeromq.ZMQ.PollItem
import java.util.concurrent.LinkedBlockingQueue


class ZStoreNetwork(val nodeName: String, config: ConfigFile.Config) extends StoreSideNetwork 
     with StoreSideReadHandler with StoreSideAllocationHandler with StoreSideTransactionHandler {
    
  import MessageEncoder._
  
  var or: Option[StoreSideReadMessageReceiver] = None
  var oa: Option[StoreSideAllocationMessageReceiver] = None
  var ot: Option[StoreSideTransactionMessageReceiver] = None
  
  def r = synchronized { or }
  def a = synchronized { oa }
  def t = synchronized { ot }
  
  val sendQueue = new LinkedBlockingQueue[ () => Unit ]()
  
  val routerEndpoint = config.nodes(nodeName).endpoint
  
  val ctx = new ZContext()
  
  val rtr = ctx.createSocket(ZMQ.ROUTER)
  rtr.setIdentity(com.ibm.aspen.util.uuid2byte(config.nodes(nodeName).uuid))
  rtr.bind(routerEndpoint)
  
  val dealers = config.nodes.foldLeft(Map[DataStoreID, Socket]()) { (m, n) =>
    val dlr = ctx.createSocket(ZMQ.DEALER)
    dlr.connect(n._2.endpoint)
    n._2.stores.foldLeft(m) { (m, s) =>
      m + (DataStoreID(config.pools(s.pool).uuid, s.store.asInstanceOf[Byte]) -> dlr)
    }
  }
  
  val receiverThread = new Thread {
    setDaemon(true)
    var heartbeat = 0
    override def run(): Unit = {
      val reactor = new ZLoop(ctx)
      val handler = new ZLoop.IZLoopHandler {
        override def handle(loop: ZLoop, item: PollItem, arg: AnyRef): Int = {
          val msg = ZMsg.recvMsg(rtr)
          
          if (msg != null) {
            
            msg.pop() // discard from address
            
            val msgFrame = msg.pop().getData()
            if (msgFrame.length == 1) {
              heartbeat += 1
              println(s"Heartbeat: $heartbeat")
            } else {
              val dataFrame = msg.pop() match {
                case null => None
                case zframe => Some(zframe.getData)
              }
              
              handleMessage(msgFrame, dataFrame)
            }
            
            msg.destroy()
          }
          
          0
        }
      }
      reactor.addPoller(new PollItem(rtr, ZMQ.Poller.POLLIN), handler, ())
      reactor.start()
    }
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
  
  def handleMessage(msgFrame: Array[Byte], dataFrame: Option[Array[Byte]]): Unit = synchronized {
    
    val p = Message.getRootAsMessage(ByteBuffer.wrap(msgFrame))
  
    if (p.prepare() != null) {
      println("got prepare")
      val message = NetworkCodec.decode(p.prepare())
      
      val sb = message.txd.allReferencedObjectsSet.foldLeft(new StringBuilder)((sb, o) => sb.append(s" ${o.uuid}"))
      println(s"got prepare txid ${message.txd.transactionUUID} for objects: ${sb.toString()}")

      val updateContent = dataFrame match {
        case None => None
        case Some(arr) => 
          var localUpdates: List[LocalUpdate] = Nil
          val bb = ByteBuffer.wrap(arr)
          
          println(s"   Prepare data length: ${bb.remaining()}")
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
  
  
  def enqueue(fn: => Unit): Unit = {
    sendQueue.add( () => fn )  
  }
  
  def send(message: TransactionMessage): Unit = enqueue {
    message match {
      case m: TxPrepareResponse => println(s"   prepare response disposition: ${m.disposition}")
      case _ => 
    }
    encodeMessage(None, message).send(dealers(message.to))
  }
  
  def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit = enqueue {
    encodeMessage(Some(client), acceptResponse).send(rtr)
  }
  
  def send(client: ClientID, resolved: TxResolved): Unit = enqueue {
    encodeMessage(Some(client), resolved).send(rtr)
  }
  
  def send(client: ClientID, finalized: TxFinalized): Unit = enqueue {
    encodeMessage(Some(client), finalized).send(rtr)
  }
  
  def sendPrepare(message: TxPrepare, updateContent: Option[List[LocalUpdate]] = None): Unit = enqueue {
    encodePrepare(message, updateContent).send(dealers(message.to))
  }
  
  def send(client: ClientID, message: allocation.ClientMessage): Unit = enqueue {
    encodeMessage(Some(client), message).send(rtr)
  }
  
  def send(message: AllocationStatusRequest): Unit = enqueue {
    encodeMessage(None, message).send(dealers(message.to))
  }
  
  def send(message: AllocationStatusReply): Unit = enqueue {
    encodeMessage(None, message).send(dealers(message.to))
  } 
  
  def send(client: ClientID, message: read.ReadResponse): Unit = enqueue {
    encodeMessage(Some(client), message).send(rtr)
  }
}