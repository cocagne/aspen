package com.ibm.aspen.demo

import com.ibm.aspen.base.StorageHost
import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer

object OnlineTracker {
  val rnd = new java.util.Random
}

class OnlineTracker(config: ConfigFile.Config) {
  
  import OnlineTracker._
  
  class NStorageHost(val cfg: ConfigFile.StorageNode) extends StorageHost {
    
    val uuid: UUID = cfg.uuid
    
    def name: String = cfg.name
    
    private var isOnline = false
  
    def online: Boolean = synchronized { isOnline }
    
    def setOnline(v: Boolean): Unit = synchronized { isOnline = v }
  
    def ownsStore(storeId: DataStoreID)(implicit ec: ExecutionContext): Future[Boolean] = Future.successful(true)
  }
  
  val nodes = config.nodes.map( n => (n._2.uuid -> new NStorageHost(n._2)) )
  
  val storeToHost = config.nodes.values.foldLeft(Map[DataStoreID, NStorageHost]()) { (m, n) =>
    
    n.stores.foldLeft(m) { (sm, s) =>
      sm + (DataStoreID(config.pools(s.pool).uuid, s.store.asInstanceOf[Byte]) -> nodes(n.uuid))
    }
  }
  
  def setNodeOnline(nodeUUID: UUID): Unit = nodes.get(nodeUUID).foreach { host => 
    synchronized {
      println(s"Node Online: ${host.name}")
      host.setOnline(true)
    }
  }
  
  def setNodeOffline(nodeUUID: UUID): Unit = nodes.get(nodeUUID).foreach { host => 
    synchronized {
      println(s"Node Offline: ${host.name}")
      host.setOnline(false) 
    }
  }
  
  def isNodeOnline(nodeUUID: UUID): Boolean = nodes(nodeUUID).online
  
  def getStorageHostForStore(storeId: DataStoreID): Future[StorageHost] = Future.successful(storeToHost(storeId))
  
  def chooseDesignatedLeader(p: ObjectPointer): Byte = {
    var attempts = 0
    var online = false
    var idx = 0.asInstanceOf[Byte]
    
    while(!online && attempts < p.ida.width * 2) {
      attempts += 1
      idx = rnd.nextInt(p.ida.width).asInstanceOf[Byte]
      online = storeToHost(DataStoreID(p.poolUUID, idx)).online
    }
    
    idx
  }
}