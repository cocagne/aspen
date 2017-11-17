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
import com.ibm.aspen.base.impl.StorageNodeMessenger
import scala.concurrent.Future
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.TxAcceptResponse
import com.ibm.aspen.core.transaction.TxFinalized
import com.ibm.aspen.base.impl.ClientMessenger
import com.ibm.aspen.core.transaction.TxPrepare
import com.ibm.aspen.core.read.ReadDriver
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.read.ReadError
import com.ibm.aspen.core.read.ObjectState
import com.ibm.aspen.core.read.ReadResponse
import scala.concurrent.Promise
import com.ibm.aspen.core.read.BaseReadDriver
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.transaction.TxResolved


class TestNetwork {
  private [this] var storeToClient = Map[DataStoreID, ClientID]()
  private [this] var storeToNode = Map[DataStoreID, StorageNode]()
  private [this] var clients = Map[ClientID, ClientMessenger]()
  
  class SNMessenger(val client: ClientID) extends StorageNodeMessenger {
    
    def initialize(node: StorageNode): Future[Unit] = {
      initializeNodeMessaging(node)
      Future.successful(())
    }
    
    def send(message: transaction.Message): Unit = storeToNode.get(message.to).foreach(sn => sn.receive(message, None))
    def send(client: ClientID, acceptResponse: TxAcceptResponse): Unit = clients.get(client).foreach(c => c.receive(acceptResponse))
    def send(client: ClientID, resolved: TxResolved): Unit = clients.get(client).foreach(c => c.receive(resolved))
    def send(client: ClientID, finalized: TxFinalized): Unit = clients.get(client).foreach(c => c.receive(finalized))
    
    def sendPrepare(message: TxPrepare, updateContent: Option[List[LocalUpdate]] = None): Unit = storeToNode.get(message.to).foreach(sn => sn.receive(message, updateContent))
    
    def send(client: ClientID, message: allocation.Message): Unit = message match {
      case m: allocation.Allocate => storeToNode.get(m.toStore).foreach(sn => sn.receive(m))
      case m: allocation.AllocateResponse => clients.get(client).foreach(c => c.receive(m))
    }
    
    def send(client: ClientID, message: read.ReadResponse, data:Option[DataBuffer]): Unit = clients.get(client).foreach(c => c.receive(message))
  }
  
  class CliMessenger(val clientId: ClientID) extends ClientMessenger {
    def send(toStore: DataStoreID, message: allocation.Allocate): Unit = storeToNode.get(toStore).foreach(sn => sn.receive(message))
    def send(toStore: DataStoreID, message: read.Read): Unit = storeToNode.get(toStore).foreach(sn => sn.receive(message))
    def send(message: TxPrepare, updateContent: List[LocalUpdate]): Unit = storeToNode.get(message.to).foreach(sn => {
      val oarr = if (updateContent.isEmpty) None else Some(updateContent)
      sn.receive(message, oarr)
    })
  }
  
  // Creates messengers for a storage node hosting the listed stores
  def addStorageNode(clientId: ClientID, dataStores: List[DataStoreID]): (ClientMessenger, StorageNodeMessenger) = {
    val cliMessenger = new CliMessenger(clientId)
    val snMessenger = new SNMessenger(clientId)
    dataStores.foreach( sid => storeToClient += (sid -> clientId) )
    clients += (clientId -> cliMessenger)
    (cliMessenger, snMessenger)
  }
  
  // Called during StorageNode initialization process
  def initializeNodeMessaging(node: StorageNode): Unit = {
    storeToClient.filter(t => t._2 == node.clientId).foreach(t => storeToNode += (t._1 -> node))
  }
  
  
}