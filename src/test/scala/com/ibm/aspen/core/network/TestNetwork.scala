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


class TestNetwork {
  
  private var stores = Map[DataStoreID, SNet]()
  private var clients = Map[ClientID, CNet]()
  
  private def add(m: CNet) = synchronized { clients += (m.clientId -> m) }
  private def add(storeId:DataStoreID, m: SNet) = synchronized { stores += (storeId -> m) }
  
  def get(storeId:DataStoreID): Option[SNet] = synchronized { stores.get(storeId) }
  def get(client: ClientID): Option[CNet] = synchronized { clients.get(client) }
  
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
    
    def send(message: transaction.Message): Unit = get(message.to).foreach(sn => sn.t.foreach(t => t.receive(message, None)))
    def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit = get(client).foreach(c => c.t.foreach(t => t.receive(acceptResponse)))
    def send(client: ClientID, resolved: TxResolved): Unit = get(client).foreach(c => c.t.foreach(t => t.receive(resolved)))
    def send(client: ClientID, finalized: TxFinalized): Unit = get(client).foreach(c => c.t.foreach(t => t.receive(finalized)))
    
    def sendPrepare(message: TxPrepare, updateContent: Option[List[LocalUpdate]] = None): Unit = get(message.to).foreach(sn => sn.t.foreach(t => t.receive(message, updateContent)))
    
    def send(client: ClientID, message: allocation.ClientMessage): Unit = message match {
      case m: allocation.Allocate => get(m.toStore).foreach(sn => sn.a.foreach(a => a.receive(m)))
      case m: allocation.AllocateResponse => get(client).foreach(c => c.a.foreach(a => a.receive(m)))
    }
    
    def send(message: AllocationStatusRequest): Unit = get(message.to).foreach(sn => sn.a.foreach(a => a.receive(message)))
    def send(message: AllocationStatusReply): Unit = get(message.to).foreach(sn => sn.a.foreach(a => a.receive(message)))
    
    def send(client: ClientID, message: read.ReadResponse, data:Option[DataBuffer]): Unit = get(client).foreach(c => c.r.foreach(r => r.receive(message)))
  }
  
  class CNet(override val clientId: ClientID) extends ClientSideNetwork 
    with ClientSideReadHandler with ClientSideAllocationHandler with ClientSideTransactionHandler {
    
    add(this)
    
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
    
    def send(toStore: DataStoreID, message: allocation.Allocate): Unit = get(toStore).foreach(sn => sn.a.foreach(a => a.receive(message)))
    def send(toStore: DataStoreID, message: read.Read): Unit = get(toStore).foreach(sn => sn.r.foreach(r=> r.receive(message)))
    def send(message: TxPrepare, updateContent: List[LocalUpdate]): Unit = get(message.to).foreach(sn => {
      val oarr = if (updateContent.isEmpty) None else Some(updateContent)
      sn.t.foreach(t => t.receive(message, oarr))
    })
  }
  
  
  
}