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

class ZStoreNetwork(val nodeName: String, config: ConfigFile.Config) extends StoreSideNetwork 
     with StoreSideReadHandler with StoreSideAllocationHandler with StoreSideTransactionHandler {
    
  import MessageEncoder._
  
  var or: Option[StoreSideReadMessageReceiver] = None
  var oa: Option[StoreSideAllocationMessageReceiver] = None
  var ot: Option[StoreSideTransactionMessageReceiver] = None
  
  def r = synchronized { or }
  def a = synchronized { oa }
  def t = synchronized { ot }
  
  val routerEndpoint = config.nodes(nodeName).endpoint
  
  val ctx = new ZContext()
  
  val rtr = ctx.createSocket(ZMQ.ROUTER)
  rtr.bind(routerEndpoint)
  
  val dealers = config.nodes.foldLeft(Map[DataStoreID, Socket]()) { (m, n) =>
    val dlr = ctx.createSocket(ZMQ.DEALER)
    dlr.connect(n._2.endpoint)
    n._2.stores.foldLeft(m) { (m, s) =>
      m + (DataStoreID(config.pools(s.pool).uuid, s.store.asInstanceOf[Byte]) -> dlr)
    }
  }
  
  val receiverThread = new Thread {
    override def run(): Unit = {
      setDaemon(true)
      
      while (true) {
        val msg = ZMsg.recvMsg(rtr)
        if (msg != null) {
          
          val p = Message.getRootAsMessage(ByteBuffer.wrap(msg.pop().getData()))
        
          if (p.prepare() != null) {
            val message = NetworkCodec.decode(p.prepare())

            val updateContent = msg.pop() match {
              case null => None
              case zframe => 
                var localUpdates: List[LocalUpdate] = Nil
                val bb = ByteBuffer.wrap(zframe.getData)
                
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
            val message = NetworkCodec.decode(p.prepareResponse())
            t.foreach(receiver => receiver.receive(message, None))
          }
          else if (p.accept() != null) {
            val message = NetworkCodec.decode(p.accept())
            t.foreach(receiver => receiver.receive(message, None))
          }
          else if (p.acceptResponse() != null) {
            val message = NetworkCodec.decode(p.acceptResponse())
            t.foreach(receiver => receiver.receive(message, None))
          }
          else if (p.resolved() != null) {
            val message = NetworkCodec.decode(p.resolved())
            t.foreach(receiver => receiver.receive(message, None))
          }
          else if (p.finalized() != null) {
            val message = NetworkCodec.decode(p.finalized())
            t.foreach(receiver => receiver.receive(message, None))
          }
          else if (p.heartbeat() != null) {
            val message = NetworkCodec.decode(p.heartbeat())
            t.foreach(receiver => receiver.receive(message, None))
          }
          else if (p.allocate() != null) {
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
        }
        msg.destroy()
      }
    }
  }
  
  receiverThread.start()
  
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
  
  
  def send(message: TransactionMessage): Unit = synchronized {
    encodeMessage(None, message).send(dealers(message.to))
  }
  
  def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit = synchronized {
    encodeMessage(Some(client), acceptResponse).send(rtr)
  }
  
  def send(client: ClientID, resolved: TxResolved): Unit = synchronized {
    encodeMessage(Some(client), resolved).send(rtr)
  }
  
  def send(client: ClientID, finalized: TxFinalized): Unit = synchronized {
    encodeMessage(Some(client), finalized).send(rtr)
  }
  
  def sendPrepare(message: TxPrepare, updateContent: Option[List[LocalUpdate]] = None): Unit = synchronized {
    encodePrepare(message, updateContent).send(dealers(message.to))
  }
  
  def send(client: ClientID, message: allocation.ClientMessage): Unit = synchronized {
    encodeMessage(Some(client), message).send(rtr)
  }
  
  def send(message: AllocationStatusRequest): Unit = synchronized {
    encodeMessage(None, message).send(dealers(message.to))
  }
  
  def send(message: AllocationStatusReply): Unit = synchronized {
    encodeMessage(None, message).send(dealers(message.to))
  } 
  
  def send(client: ClientID, message: read.ReadResponse): Unit = synchronized {
    encodeMessage(Some(client), message).send(rtr)
  }
}