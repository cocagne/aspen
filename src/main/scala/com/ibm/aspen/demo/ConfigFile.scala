package com.ibm.aspen.demo

import com.ibm.aspen.demo.YamlFormat._
import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.ida.Replication
import java.io.File
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor
import java.io.FileInputStream
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.base.impl.Bootstrap

/* 
 pools:
  - name: bootstrap-pool
    width: 5
    uuid: 65da619d-8e80-47dd-be70-98c3776ce0cd
    
  - name: inode-pool
    width: 5
    
  - name: file-data-pool
    width: 5
    
object-allocaters:
  - name: inode-allocater
    pool: inode-pool
    ida: 
       type: replication 
       width: 5
       writeThreshold: 3
    
storage-nodes:
  - name: node_a
    endpoint: tcp://127.0.0.1:5001
    crl:
       type: rocksdb
       path  /var/lib/aspen/node_a/crl
    stores:
       - pool: bootstrap
         store: 0
         type: rocksdb
         path:  /var/lib/aspen/node_a/bootstrap-0*/
object ConfigFile {
  
  object ReplicationFormat extends YObject[IDA] {
    val width           = Required("width",            YInt)
    val writeThreshold  = Required("write-threshold",  YInt)
    
    val attrs = width :: writeThreshold :: Nil
    
    def create(o: Object): IDA = Replication(width.get(o), writeThreshold.get(o))
  }
  
  val IDAOptions =  Map(("replication" -> ReplicationFormat))
  
  
  abstract class MissedUpdateHandler
  
  case class PerStoreSet(allocaters: List[String], nodeSizes: List[Int], nodeLimits: List[Int]) extends MissedUpdateHandler
  
  object PerStoreSet extends YObject[MissedUpdateHandler] {
    
    val allocaters = Required("tier-allocaters",   YList(YString))
    val nodeSizes = Required("tier-node-sizes",    YList(YInt))
    val nodeLimits = Required("tier-node-kv-limit", YList(YInt))
    
    val attrs = allocaters :: nodeSizes :: nodeLimits :: Nil
    
    def create(o: Object): PerStoreSet = PerStoreSet(allocaters.get(o), nodeSizes.get(o), nodeLimits.get(o))
  }
  
  val MissedUpdateOptions = Map(("per-store-set" -> PerStoreSet))
  
  case class Pool(name: String, width: Int, uuid: UUID, missedUpdateStrategy: MissedUpdateHandler)
  
  object Pool extends YObject[Pool] {
    val name                 = Required("name",  YString)
    val width                = Required("width", YInt)
    val uuid                 = Required("uuid",  YUUID)
    val missedUpdateStrategy = Required("missed-update-strategy", Choice("type", MissedUpdateOptions))
    
    val attrs = name :: width :: uuid :: missedUpdateStrategy :: Nil
    
    def create(o: Object): Pool = Pool(name.get(o), width.get(o), uuid.get(o), missedUpdateStrategy.get(o))
  }
  
  
  
  case class ObjectAllocater(name: String, pool: String, uuid: UUID, ida: IDA, maxObjectSize: Option[Int])
  
  object ObjectAllocater extends YObject[ObjectAllocater] {
    val name          = Required("name", YString)
    val pool          = Required("pool", YString)
    val uuid          = Required("uuid", YUUID)
    val ida           = Required("ida",  Choice("type", IDAOptions))
    val maxObjectSize = Optional("max-object-size", YInt)
    
    val attrs = name :: pool :: uuid :: ida :: maxObjectSize :: Nil
    
    def create(o: Object): ObjectAllocater = ObjectAllocater(name.get(o), pool.get(o), uuid.get(o), ida.get(o), maxObjectSize.get(o))
  }
  
  sealed abstract class StorageBackend
  
  case class RocksDB(path: String) extends StorageBackend
  
  object RocksDB extends YObject[RocksDB] {
    val path = Required("path", YString)
    val attrs = path :: Nil
    
    def create(o: Object): RocksDB = RocksDB(path.get(o))
  }
  
  case class DataStore(pool: String, store: Int, backend: StorageBackend)
  
  object DataStore extends YObject[DataStore] {
    val pool    = Required("pool",     YString)
    val store   = Required("store",    YInt)
    val backend = Required("backend", Choice("storage-engine", Map(("rocksdb" -> RocksDB))))
    
    val attrs = pool :: store :: backend :: Nil
    
    def create(o: Object): DataStore = DataStore(pool.get(o), store.get(o), backend.get(o))
  }
  
  case class Endpoint(host: String, port: Int)
  
  object Endpoint extends YObject[Endpoint] {
    val host = Required("host", YString)
    val port = Required("port", YInt)

    val attrs = host :: port :: Nil
    
    def create(o: Object): Endpoint = Endpoint(host.get(o), port.get(o))
  }
  
  case class StorageNode(name: String, uuid: UUID, endpoint: Endpoint, log4jConfigFile: File, crl: StorageBackend, stores: List[DataStore])
  
  object StorageNode extends YObject[StorageNode] {
    val name      = Required("name",         YString)
    val uuid      = Required("uuid",         YUUID)
    val endpoint  = Required("endpoint",     Endpoint)
    val log4jConf = Required("log4j-config", YFile)
    val crl       = Required("crl",          Choice("storage-engine", Map(("rocksdb" -> RocksDB))))
    val stores    = Required("stores",       YList(DataStore))
    
    val attrs = name :: uuid :: endpoint :: log4jConf :: crl :: stores :: Nil
    
    def create(o: Object): StorageNode = StorageNode(name.get(o), uuid.get(o), endpoint.get(o), log4jConf.get(o), crl.get(o), stores.get(o))
  }
  
  object YStorePointer  extends YObject[StorePointer] {
    val poolIndex = Required("pool-index", YInt)
    val data      = Optional("data",       YString)
    
    val attrs = poolIndex :: data :: Nil
    
    def create(o: Object): StorePointer = {
      val idx = poolIndex.get(o).asInstanceOf[Byte]
      val arr = data.get(o) match {
        case None => new Array[Byte](0)
        case Some(s) => java.util.Base64.getDecoder.decode(s)
      }
      new StorePointer(idx, arr)
    }
  }
  
  object RadiclePointer extends YObject[KeyValueObjectPointer] {
    val uuid          = Required("uuid",           YUUID)
    val poolUUID      = Required("pool-uuid",      YUUID)
    val size          = Optional("size",           YInt)
    val ida           = Required("ida",            Choice("type", Map(("replication" -> ReplicationFormat))))
    val storePointers = Required("store-pointers", YList(YStorePointer))
    
    val attrs = uuid :: poolUUID :: size :: ida :: storePointers :: Nil
    
    def create(o: Object): KeyValueObjectPointer = KeyValueObjectPointer(uuid.get(o), poolUUID.get(o), size.get(o), ida.get(o), storePointers.get(o).toArray)
  }
  
  def zeroeduuid(uuid: UUID): Boolean = uuid.getMostSignificantBits == 0 && uuid.getLeastSignificantBits == 0
  
  case class Config(pools: Map[String, Pool], allocaters: Map[String, ObjectAllocater], nodes: Map[String, StorageNode], oradicle: Option[KeyValueObjectPointer]) {
    // Validate config
    {
      allocaters.values.foreach { a =>
        if (!pools.contains(a.pool))
          throw new FormatError(s"Object Allocater ${a.name} references unknown pool ${a.pool}")
      }
      
      if (!allocaters.contains("bootstrap-allocater"))
        throw new FormatError("Missing Required Object Allocater: 'bootstrap-allocater'")
      
      if (!pools.contains("bootstrap-pool"))
        throw new FormatError("Missing Required Pool: 'bootstrap-pool'")
      
      if (!zeroeduuid(pools("bootstrap-pool").uuid))
        throw new FormatError("bootstrap-pool must use a zeroed UUID")
      
      if (allocaters("bootstrap-allocater").pool != "bootstrap-pool")
        throw new FormatError("bootstrap-allocater must use the bootstrap-pool")
      
      if (!zeroeduuid(allocaters("bootstrap-allocater").uuid))
        throw new FormatError("bootstrap-allocater must use a zeroed UUID")
      
      allocaters.values.foreach { a =>
        if (!pools.contains(a.pool))
          throw new FormatError(s"Allocater ${a.name} references unknown pool ${a.pool}")
      }
      
      pools.values.foreach { p =>
        p.missedUpdateStrategy match {
          case pss: PerStoreSet => 
            if (pss.allocaters.length == 0)
              throw new FormatError(s"tier-allocaters attribute of pool ${p.name} must contain at least one object allocater name")
            if (pss.nodeSizes.length == 0)
              throw new FormatError(s"tier-node-sizes attribute of pool ${p.name} must contain at least one node size (in bytes)")
            pss.allocaters.foreach { allocaterName =>
              if (!allocaters.contains(allocaterName))
                throw new FormatError(s"pool ${p.name} references unknown allocater ${allocaterName}")
            }
        }
      }
      
      nodes.values.foreach { n =>
        n.stores.foreach { s =>
          if (!pools.contains(s.pool))
            throw new FormatError(s"Storage Node ${n.name} references unknown pool ${s.pool}")
        }
      }
      
      val allStores = pools.values.foldLeft(Set[String]()) { (s, p) =>
        (0 until p.width).foldLeft(s)( (s, i) => s + s"${p.name}:$i" )
      }
      val ownedStores = nodes.values.foldLeft(Set[String]()) { (s, n) =>
        n.stores.foldLeft(s)( (s, st) => s + s"${st.pool}:${st.store}" )
      }
      val missing = allStores &~ ownedStores
      val extra = ownedStores &~ allStores
      
      if (!missing.isEmpty) throw new FormatError(s"Unowned DataStore(s): $missing")
      if (!extra.isEmpty) throw new FormatError(s"Undefined DataStore(s): $extra")
      
      val uuids = pools.values.map(p => p.uuid) ++ allocaters.values.map(a => a.uuid) ++ nodes.values.map(n => n.uuid)
      
      uuids.foldLeft(Set[UUID]()) { (s, u) =>
        if (s.contains(u) && !zeroeduuid(u))
          throw new FormatError(s"Duplicated UUID: $u")
        s + u
      }
    }
  }
  
  object Config extends YObject[Config] {
    val pools      = Required("pools",             YList(Pool))
    val allocaters = Required("object-allocaters", YList(ObjectAllocater))
    val nodes      = Required("storage-nodes",     YList(StorageNode))
    val radicle    = Optional("radicle",           RadiclePointer)
    
    val attrs = pools :: allocaters :: nodes :: radicle :: Nil
    
    def create(o: Object): Config = {
      Config( 
          pools.get(o).map(p => (p.name -> p)).toMap, 
          allocaters.get(o).map(p => (p.name -> p)).toMap, 
          nodes.get(o).map(p => (p.name -> p)).toMap,
          radicle.get(o))
    }
  }
  
  def loadConfig(file: File): Config = {
    val yaml = new Yaml(new SafeConstructor)
    val y = yaml.load[java.util.AbstractMap[Object,Object]](new FileInputStream(file))
    Config.create(y)
  }
}