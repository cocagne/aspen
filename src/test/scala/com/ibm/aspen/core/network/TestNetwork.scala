package com.ibm.aspen.core.network

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction._
import com.ibm.aspen.core.{TestActionContext, allocation, read, transaction}

import scala.concurrent.Future

class TestNetwork(otac: Option[TestActionContext] = None) {

  val tac: TestActionContext = otac.getOrElse(new TestActionContext)

  private var stores = Map[DataStoreID, SNet]()
  private var clients = Map[ClientID, CNet]()

  def setSystem(sys: AspenSystem): Unit = {
    stores.valuesIterator.foreach(_.setSystem(sys))
    clients.valuesIterator.foreach(_.setSystem(sys))
  }

  def shutdown(): Future[Unit] = if (otac.isEmpty) tac.shutdown() else Future.unit

  private def add(m: CNet): Unit = synchronized { clients += (m.clientId -> m) }
  private def add(storeId:DataStoreID, m: SNet): Unit = synchronized { stores += (storeId -> m) }
  
  def get(storeId:DataStoreID): Option[SNet] = synchronized { stores.get(storeId) }
  def get(client: ClientID): Option[CNet] = synchronized { clients.get(client) }


  private def ssend(storeId: DataStoreID, fn: SNet => Any): Unit = synchronized {
    stores.get(storeId).foreach { snet => tac.act {
      fn(snet)
    }}
  }

  private def csend(client: ClientID, fn: CNet => Any): Unit = synchronized {
    clients.get(client).foreach { cnet => tac.act {
      fn(cnet)
    }}
  }
  
  class SNet extends StoreSideNetwork 
     with StoreSideReadHandler with StoreSideAllocationHandler with StoreSideTransactionHandler {

    private[this] var connected: Boolean = true

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

    def disconnectNetwork(): Unit = synchronized( connected = false )
    def connectNetwork(): Unit = synchronized( connected = true )
    
    private[this] var or: Option[StoreSideReadMessageReceiver] = None
    private[this] var oa: Option[StoreSideAllocationMessageReceiver] = None
    private[this] var ot: Option[StoreSideTransactionMessageReceiver] = None
    
    def r: Option[StoreSideReadMessageReceiver] = synchronized { if (connected) or else None }
    def a: Option[StoreSideAllocationMessageReceiver] = synchronized { if (connected) oa else None }
    def t: Option[StoreSideTransactionMessageReceiver] = synchronized { if (connected) ot else None }
    
    def setReceiver(receiver: StoreSideReadMessageReceiver): Unit = synchronized { or = Some(receiver) }
    def setReceiver(receiver: StoreSideAllocationMessageReceiver): Unit = synchronized { oa = Some(receiver) }
    def setReceiver(receiver: StoreSideTransactionMessageReceiver): Unit = synchronized { ot = Some(receiver) }


    def send(message: transaction.Message): Unit = ssend(message.to, _.t.foreach(_.receive(message, None)))

    def send(client: ClientID, prepareResponse: TxPrepareResponse): Unit = csend(client, _.t.foreach(_.receive(prepareResponse)))

    def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit = csend(client, _.t.foreach(_.receive(acceptResponse)))

    def send(client: ClientID, resolved: TxResolved): Unit = csend(client, _.t.foreach(t => t.receive(resolved)))
    def send(client: ClientID, finalized: TxFinalized): Unit = csend(client, _.t.foreach(t => t.receive(finalized)))
    
    def sendPrepare(message: TxPrepare, transactionData: Option[TransactionData] = None): Unit = {
      ssend(message.to, _.t.foreach(t => t.receive(message, transactionData)))
    }

    override def send(messages: List[Message]): Unit = messages.foreach(msg => ssend(msg.to, _.t.foreach(_.receive(msg, None))))

    override def sendPrepares(messages: List[(TxPrepare, Option[TransactionData])]): Unit = {
      messages.foreach { tpl =>
        val (prep, transactionData) = tpl
        ssend(prep.to, _.t.foreach(_.receive(prep, transactionData)))
      }
    }
    
    def send(client: ClientID, message: allocation.ClientMessage): Unit = message match {
      case m: allocation.Allocate => ssend(m.toStore, _.a.foreach(a => a.receive(m)))
      case m: allocation.AllocateResponse => csend(client, _.a.foreach(a => a.receive(m)))
    }

    def send(client: ClientID, message: read.ReadResponse): Unit = csend(client, _.r.foreach(r => r.receive(message)))
    def send(client: ClientID, message: read.TransactionCompletionResponse): Unit = csend(client, _.r.foreach(r => r.receive(message)))
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
    
    private[this] var or: Option[ClientSideReadMessageReceiver] = None
    private[this] var oa: Option[ClientSideAllocationMessageReceiver] = None
    private[this] var ot: Option[ClientSideTransactionMessageReceiver] = None
    
    def r: Option[ClientSideReadMessageReceiver] = synchronized { or }
    def a: Option[ClientSideAllocationMessageReceiver] = synchronized { oa }
    def t: Option[ClientSideTransactionMessageReceiver] = synchronized { ot }
    
    def setReceiver(receiver: ClientSideReadMessageReceiver): Unit = synchronized { or = Some(receiver) }
    def setReceiver(receiver: ClientSideAllocationMessageReceiver): Unit = synchronized { oa = Some(receiver) }
    def setReceiver(receiver: ClientSideTransactionMessageReceiver): Unit = synchronized { ot = Some(receiver) }
    
    def send(toStore: DataStoreID, message: allocation.Allocate): Unit = ssend(toStore, _.a.foreach(a => a.receive(message)))
    def send(message: read.Read): Unit = ssend(message.toStore, _.r.foreach(r=> r.receive(message)))
    def send(message: read.OpportunisticRebuild): Unit = ssend(message.toStore, _.r.foreach(r=> r.receive(message)))
    def send(message: read.TransactionCompletionQuery): Unit = ssend(message.toStore, _.r.foreach(r=> r.receive(message)))
    def send(message: TxPrepare, transactionData: TransactionData): Unit = ssend(message.to, sn => {
      sn.t.foreach(t => t.receive(message, Some(transactionData)))
    })
  }

}