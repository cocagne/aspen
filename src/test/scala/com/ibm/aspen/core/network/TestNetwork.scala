package com.ibm.aspen.core.network

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import com.ibm.aspen.core.read
import com.ibm.aspen.core.allocation
import com.ibm.aspen.core.transaction
import com.ibm.aspen.core.data_store.DataStoreID
import java.util.UUID

import com.ibm.aspen.base.impl.StorageNode

import scala.concurrent.Future
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.read.ReadDriver
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.read.ReadError
import com.ibm.aspen.core.read.ReadResponse

import scala.concurrent.Promise
import com.ibm.aspen.core.read.BaseReadDriver
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.transaction.TxResolved
import com.ibm.aspen.core.allocation.AllocationStatusRequest
import com.ibm.aspen.core.allocation.AllocationStatusReply
import com.ibm.aspen.core.transaction.TxPrepareResponse

class TestNetwork {

  private var stores = Map[DataStoreID, SNet]()
  private var clients = Map[ClientID, CNet]()

  sealed abstract class NetAction

  case class Shutdown() extends NetAction

  case class ClientMessage(client: ClientID, deliver: CNet => Unit) extends NetAction
  case class StoreMessage(storeId: DataStoreID, deliver: SNet => Unit) extends NetAction

  private val actionQueue = new LinkedBlockingQueue[NetAction]()
  private val shutdownPromise = Promise[Unit]()

  private val deliveryThread = new Thread(() => bgThread(), "TestNetwork Message Deliver")

  deliveryThread.start()

  def bgThread(): Unit = {
    var done = false
    while (!done) {
      actionQueue.take() match {
        case _:Shutdown =>
          done = true
          shutdownPromise.success(())
        case m: ClientMessage =>
          synchronized(get(m.client)).foreach(m.deliver)
        case m: StoreMessage =>
          synchronized(get(m.storeId)).foreach(m.deliver)
      }
    }
  }

  def setSystem(sys: AspenSystem): Unit = {
    stores.valuesIterator.foreach(_.setSystem(sys))
    clients.valuesIterator.foreach(_.setSystem(sys))
  }

  def shutdown(): Future[Unit] = {
    actionQueue.put(Shutdown())
    shutdownPromise.future
  }

  private def add(m: CNet) = synchronized { clients += (m.clientId -> m) }
  private def add(storeId:DataStoreID, m: SNet) = synchronized { stores += (storeId -> m) }
  
  def get(storeId:DataStoreID): Option[SNet] = synchronized { stores.get(storeId) }
  def get(client: ClientID): Option[CNet] = synchronized { clients.get(client) }


  private def cdeliver(client: ClientID, fn: CNet => Unit): Unit = actionQueue.put(ClientMessage(client, fn))
  private def sdeliver(storeId: DataStoreID, fn: SNet => Unit): Unit = actionQueue.put(StoreMessage(storeId, fn))

  
  class SNet extends StoreSideNetwork 
     with StoreSideReadHandler with StoreSideAllocationHandler with StoreSideTransactionHandler {
    
    // -----------------------------------------------------
    // StoreSideNetwork
    val readHandler: StoreSideReadHandler = this
    val allocationHandler: StoreSideAllocationHandler = this
    val transactionHandler: StoreSideTransactionHandler = this
    
    /** Called when a storage node begins hosting a data store */
    def registerHostedStore(storeId: DataStoreID): Unit = add(storeId, this)
    
    /** Called when a storage node stops hosting a data store */
    def unregisterHostedStore(storeId: DataStoreID): Unit = ()

    private[this] var osystem: Option[AspenSystem] = None

    def system: Option[AspenSystem] = synchronized(osystem)

    def setSystem(s: AspenSystem): Unit = synchronized(osystem = Some(s))
    //-------------------------------------------------------
    
    
    var or: Option[StoreSideReadMessageReceiver] = None
    var oa: Option[StoreSideAllocationMessageReceiver] = None
    var ot: Option[StoreSideTransactionMessageReceiver] = None
    
    def r = synchronized { or }
    def a = synchronized { oa }
    def t = synchronized { ot }
    
    def setReceiver(receiver: StoreSideReadMessageReceiver): Unit = synchronized { or = Some(receiver) }
    def setReceiver(receiver: StoreSideAllocationMessageReceiver): Unit = synchronized { oa = Some(receiver) }
    def setReceiver(receiver: StoreSideTransactionMessageReceiver): Unit = synchronized { ot = Some(receiver) }
    
    def send(message: transaction.Message): Unit = sdeliver(message.to, _.t.foreach(t => t.receive(message, None)))
    def send(client: ClientID, prepareResponse: TxPrepareResponse): Unit = cdeliver(client, _.t.foreach(t => t.receive(prepareResponse)))
    def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit = cdeliver(client,_.t.foreach(t => t.receive(acceptResponse)))
    def send(client: ClientID, resolved: TxResolved): Unit = cdeliver(client, _.t.foreach(t => t.receive(resolved)))
    def send(client: ClientID, finalized: TxFinalized): Unit = cdeliver(client, _.t.foreach(t => t.receive(finalized)))
    
    def sendPrepare(message: TxPrepare, updateContent: Option[List[LocalUpdate]] = None): Unit = {
      sdeliver(message.to, _.t.foreach(t => t.receive(message, updateContent)))
    }
    
    def send(client: ClientID, message: allocation.ClientMessage): Unit = message match {
      case m: allocation.Allocate => sdeliver(m.toStore, _.a.foreach(a => a.receive(m)))
      case m: allocation.AllocateResponse => cdeliver(client, _.a.foreach(a => a.receive(m)))
    }
    
    def send(message: AllocationStatusRequest): Unit = sdeliver(message.to, _.a.foreach(a => a.receive(message)))
    def send(message: AllocationStatusReply): Unit = sdeliver(message.to, _.a.foreach(a => a.receive(message)))
    
    def send(client: ClientID, message: read.ReadResponse): Unit = cdeliver(client, _.r.foreach(r => r.receive(message)))
  }
  
  class CNet(override val clientId: ClientID) extends ClientSideNetwork 
    with ClientSideReadHandler with ClientSideAllocationHandler with ClientSideTransactionHandler {
    
    add(this)

    private[this] var osystem: Option[AspenSystem] = None

    def system: Option[AspenSystem] = synchronized(osystem)

    def setSystem(s: AspenSystem): Unit = synchronized(osystem = Some(s))
    
    val readHandler: ClientSideReadHandler = this
    val allocationHandler: ClientSideAllocationHandler = this
    val transactionHandler: ClientSideTransactionHandler = this
    
    var or: Option[ClientSideReadMessageReceiver] = None
    var oa: Option[ClientSideAllocationMessageReceiver] = None
    var ot: Option[ClientSideTransactionMessageReceiver] = None
    
    def r = synchronized { or }
    def a = synchronized { oa }
    def t = synchronized { ot }
    
    def setReceiver(receiver: ClientSideReadMessageReceiver): Unit = synchronized { or = Some(receiver) }
    def setReceiver(receiver: ClientSideAllocationMessageReceiver): Unit = synchronized { oa = Some(receiver) }
    def setReceiver(receiver: ClientSideTransactionMessageReceiver): Unit = synchronized { ot = Some(receiver) }
    
    def send(toStore: DataStoreID, message: allocation.Allocate): Unit = sdeliver(toStore, _.a.foreach(a => a.receive(message)))
    def send(message: read.Read): Unit = sdeliver(message.toStore, _.r.foreach(r=> r.receive(message)))
    def send(message: read.OpportunisticRebuild): Unit = sdeliver(message.toStore, _.r.foreach(r=> r.receive(message)))
    def send(message: TxPrepare, updateContent: List[LocalUpdate]): Unit = sdeliver(message.to, sn => {
      val oarr = if (updateContent.isEmpty) None else Some(updateContent)
      sn.t.foreach(t => t.receive(message, oarr))
    })
  }

}