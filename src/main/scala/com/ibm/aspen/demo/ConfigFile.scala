package com.ibm.aspen.demo

import com.ibm.aspen.demo.YamlFormat._
import java.util.UUID
import com.ibm.aspen.core.ida.IDA
import com.ibm.aspen.core.ida.Replication

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
  
  case class Pool(name: String, width: Int, uuid: UUID)
  
  object Pool extends YObject[Pool] {
    val name  = Required("name",  YString)
    val width = Required("width", YInt)
    val uuid  = Required("uuid",  YUUID)
    
    val attrs = name :: width :: uuid :: Nil
    
    def create(o: Object): Pool = Pool(name.get(o), width.get(o), uuid.get(o))
  }
  
  object ReplicationFormat extends YObject[IDA] {
    val width           = Required("width",            YInt)
    val writeThreshold  = Required("write-threshold",  YInt)
    
    val attrs = width :: writeThreshold :: Nil
    
    def create(o: Object): IDA = Replication(width.get(o), writeThreshold.get(o))
  }
  
  case class ObjectAllocater(name: String, pool: String, uuid: UUID, ida: IDA)
  
  object ObjectAllocater extends YObject[ObjectAllocater] {
    val name = Required("name", YString)
    val pool = Required("pool", YString)
    val uuid = Required("uuid", YUUID)
    val ida  = Choice("ida", Map(("replication" -> ReplicationFormat)))
    
    val attrs = name :: pool :: uuid :: ida :: Nil
    
    def create(o: Object): ObjectAllocater = ObjectAllocater(name.get(o), pool.get(o), uuid.get(o), ida.get(o))
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
    val pool    = Required("pool", YString)
    val store   = Required("store", YInt)
    val backend = Choice("backend", Map(("rocksdb" -> RocksDB)))
    
    val attrs = pool :: store :: backend :: Nil
    
    def create(o: Object): DataStore = DataStore(pool.get(o), store.get(o), backend.get(o))
  }
  
  case class StorageNode(name: String, uuid: UUID, endpoint: String, crl: StorageBackend, stores: List[DataStore])
  
  object StorageNode extends YObject[StorageNode] {
    val name     = Required("name", YString)
    val uuid     = Required("uuid", YUUID)
    val endpoint = Required("endpoint", YString)
    val crl      = Choice("backend", Map(("rocksdb" -> RocksDB)))
    val stores   = Required("stores", YList(DataStore))
    
    val attrs = name :: uuid :: endpoint :: crl :: stores :: Nil
    
    def create(o: Object): StorageNode = StorageNode(name.get(o), uuid.get(o), endpoint.get(o), crl.get(o), stores.get(o))
  }
  
  case class Config(pools: Map[String, Pool], allocaters: Map[String, ObjectAllocater], nodes: Map[String, StorageNode]) {
    // Validate config
    {
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
        if (s.contains(u))
          throw new FormatError(s"Duplicated UUID: $u")
        s + u
      }
    }
  }
  
  object Config extends YObject[Config] {
    val pools = Required("pools", YList(Pool))
    val allocaters = Required("object-allocaters", YList(ObjectAllocater))
    val nodes = Required("storage-nodes", YList(StorageNode))
    
    val attrs = pools :: allocaters :: nodes :: Nil
    
    def create(o: Object): Config = {
      Config( 
          pools.get(o).map(p => (p.name -> p)).toMap, 
          allocaters.get(o).map(p => (p.name -> p)).toMap, 
          nodes.get(o).map(p => (p.name -> p)).toMap)
    }
  }
  
  
}