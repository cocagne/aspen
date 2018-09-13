package com.ibm.aspen.demo

import java.io.File

import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.RocksDBDataStoreBackend
import com.ibm.aspen.core.data_store.DataStoreFrontend
import com.ibm.aspen.base.impl.Bootstrap

import scala.concurrent.Await
import scala.concurrent.duration._
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.ida.ReedSolomon
import com.ibm.aspen.base.impl.RocksDBCrashRecoveryLog
import java.util.UUID

import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.base.impl.BasicAspenSystem
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.read.BaseReadDriver

import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.transaction.ClientTransactionDriver
import com.ibm.aspen.core.allocation.BaseAllocationDriver
import com.ibm.aspen.base.impl.BaseTransaction
import com.ibm.aspen.base.impl.BaseStoragePool
import com.ibm.aspen.base.NoRetry
import com.ibm.aspen.base.impl.StorageNode
import com.ibm.aspen.core.data_store.DataStore
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.allocation.AllocationRecoveryState

import scala.concurrent.Future
import com.ibm.aspen.cumulofs.FileSystem

import scala.concurrent.ExecutionContext.Implicits.global
import com.ibm.aspen.cumulofs.FileInode
import com.ibm.aspen.cumulofs.impl.{CumuloFSTypeRegistry, FuseInterface, SimpleFileSystem}
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.fuse.FuseOptions
import com.ibm.aspen.fuse.RemoteFuse
import com.ibm.aspen.base.impl.BaseImplTypeRegistry
import com.ibm.aspen.base.impl.BaseTransactionFinalizer
import com.ibm.aspen.base.impl.StorageNodeTransactionManager
import com.ibm.aspen.base.impl.StorageNodeAllocationManager
import com.ibm.aspen.core.transaction.TransactionDriver
import com.ibm.aspen.base.impl.SuperSimpleRetryingReadDriver
import com.ibm.aspen.base.ExponentialBackoffRetryStrategy
import com.ibm.aspen.base.impl.SimpleStorageNodeTxManager
import com.ibm.aspen.base.impl.SimpleFixedDelayTransactionDriver
import com.ibm.aspen.base.impl.SimpleStorageNodeAllocationManager
import com.ibm.aspen.base.impl.SimpleClientTransactionDriver
import com.ibm.aspen.base.impl.SuperSimpleRetryingAllocationDriver
import com.ibm.aspen.base.impl.PerStoreMissedUpdate
import com.ibm.aspen.base.AllocatedObjectsIterator

import scala.concurrent.Promise
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.core.data_store.ObjectMetadata
import com.ibm.aspen.core.data_store.StoreObjectID
import com.ibm.aspen.base.ObjectAllocaterFactory
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.base.impl.SinglePoolObjectAllocater
import com.ibm.aspen.base.AggregateTypeRegistry
import com.ibm.aspen.base.TypeFactory
import com.ibm.aspen.base.TypeRegistry
import org.apache.logging.log4j.scala.Logging
import com.ibm.aspen.core.objects.keyvalue.Insert

object Main {
  
  val CumuloFSKey = Key("cumulofs")
  
  case class Args(mode:String="", configFile:File=null, log4jConfigFile: File=null, nodeName:String="", host:String="", port:Int=0)
 
  class ConfigError(msg: String) extends Exception(msg)
  
  def setLog4jConfigFile(f: File): Unit = {
    //System.setProperty("log4j2.debug", "true")
    
    // Set all loggers to Asynchronous Logging
    System.setProperty("log4j2.contextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector")
    System.setProperty("log4j2.configurationFile", s"file:${f.getAbsolutePath}")
  }
  
  def main(args: Array[String]) {    
    val parser = new scopt.OptionParser[Args]("demo") {
      head("demo", "0.1")
      
      cmd("bootstrap").text("Bootstrap a new Aspen system").
        action( (_,c) => c.copy(mode="bootstrap")).
        children(
            arg[File]("<config-file>").text("Configuration File").
              action( (x, c) => c.copy(configFile=x)).
              validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x"))
        )
        
      cmd("node").text("Starts an Aspen Storage Node").
        action( (_,c) => c.copy(mode="node")).
        children(
            arg[File]("<config-file>").text("Configuration File").
              action( (x, c) => c.copy(configFile=x)).
              validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
            
            arg[String]("<node-name>").text("Storage Node Name").action((x,c) => c.copy(nodeName=x))
        )
        
      cmd("cumulofs").text("Launches a CumuloFS server").
        action( (_,c) => c.copy(mode="cumulofs")).
        children(
            arg[File]("<config-file>").text("Aspen Configuration File").
              action( (x, c) => c.copy(configFile=x)).
              validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
              
            arg[File]("<log4j-config-file>").text("Log4j Configuration File").
              action( (x, c) => c.copy(log4jConfigFile=x)).
              validate( x => if (x.exists()) success else failure(s"Log4j Config file does not exist: $x")),
              
            arg[String]("<host>").text("Storage Node Name").action((x,c) => c.copy(host=x)),
            
            arg[Int]("<port>").text("Storage Node Name").action((x,c) => c.copy(port=x))
        )
        
       cmd("rebuild").text("Rebuilds a store").
         action( (_,c) => c.copy(mode="rebuild")).
        children(
            arg[File]("<config-file>").text("Configuration File").
              action( (x, c) => c.copy(configFile=x)).
              validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
            
            arg[String]("<store-name>").text("Data Store Name. Format is \"pool-name:storeNumber\"").
              action((x,c) => c.copy(nodeName=x)).
              validate { x => 
                val arr = x.split(":")
                if (arr.length == 2) {
                  try {
                    Integer.parseInt(arr(1))
                    success
                  } catch {
                    case t: Throwable => failure("Store name must match the format \"pool-name:storeNumber\"")
                  }
                }
                else failure("Store name must match the format \"pool-name:storeNumber\"")
              }
       )
        
      checkConfig( c => if (c.mode == "") failure("Invalid command") else success )
    }
    
    parser.parse(args, Args()) match {
      case Some(cfg) =>
        println(s"Successful config: $cfg")
        try {
          val config = ConfigFile.loadConfig(cfg.configFile)
          //println(s"Config file: $config")
          cfg.mode match {
            case "bootstrap" => bootstrap(config)
            case "node" => node(cfg.nodeName, config)
            case "cumulofs" => cumulofs(cfg.log4jConfigFile, config)
            case "rebuild" => rebuild(cfg.nodeName, config)
          }
        } catch {
          case e: YamlFormat.FormatError => println(s"Error loading config file: $e")
          case e: ConfigError => println(s"Error: $e")
        }
      case None =>
    }
  }
  
  def createSystem(cfg: ConfigFile.Config, onnet: Option[NettyNetwork]=None): BasicAspenSystem = {
    val radiclePointer = cfg.oradicle.getOrElse(throw new ConfigError("Radicle Pointer is missing from the config file!"))
    
    val bootstrapPoolIDA = cfg.allocaters("bootstrap-allocater").ida

    val nnet = onnet.getOrElse(new NettyNetwork(cfg))
    val cliNet = nnet.createClientNetwork()
    
    val prepareRetransmitDelay = Duration(1, SECONDS)
    val allocationRetransmitDelay = Duration(1, SECONDS)
    val opportunisticRebuildDelay = Duration(3, SECONDS)
    
    val allocaterFactories = cfg.allocaters.values.foldLeft(Map[UUID,TypeFactory]()){ (m, a) => 
      m + (a.uuid -> new ObjectAllocaterFactory {
        val typeUUID = a.uuid
        def create(system: AspenSystem)(implicit ec: ExecutionContext): Future[ObjectAllocater] = {
          Future.successful(new SinglePoolObjectAllocater(system, a.uuid, cfg.pools(a.pool).uuid, a.maxObjectSize, a.ida))
        }
      })
    }
    
    val allocaterRegistry = new TypeRegistry {
      def getTypeFactory[T <: TypeFactory](typeUUID: UUID): Option[T] = allocaterFactories.get(typeUUID) match {
        case None => None
        case Some(t) => 
          try {
            Some(t.asInstanceOf[T])
          } catch {
            case e: ClassCastException => None
          }
      }
    }
    
    val typeRegistry = new AggregateTypeRegistry(CumuloFSTypeRegistry :: allocaterRegistry :: Nil)
    
    new BasicAspenSystem(
        chooseDesignatedLeader = cliNet.onlineTracker.chooseDesignatedLeader _,
        getStorageHostFn = cliNet.onlineTracker.getStorageHostForStore _,
        net = cliNet,
        defaultReadDriverFactory = SuperSimpleRetryingReadDriver.factory(opportunisticRebuildDelay, ExecutionContext.Implicits.global) _,
        defaultTransactionDriverFactory = SimpleClientTransactionDriver.factory(prepareRetransmitDelay),
        defaultAllocationDriverFactory = SuperSimpleRetryingAllocationDriver.factory(allocationRetransmitDelay),
        transactionFactory = BaseTransaction.Factory,
        storagePoolFactory = BaseStoragePool.Factory,
        bootstrapPoolIDA = bootstrapPoolIDA,
        radiclePointer = radiclePointer,
        retryStrategy = new ExponentialBackoffRetryStrategy(backoffLimit = 10, initialRetryDelay = 1),
        userTypeRegistry = Some(typeRegistry)
        )
  }
  
  def initializeCumulofs(sys: BasicAspenSystem, numIndexNodeSegments: Int = 100, fileSegmentSize:Int=1024*1024): Future[FileSystem] = {
    
    // Approximate the size of the node needed to store numSegments
    val nodeSize = (sys.radiclePointer.encodedSize + 2) * numIndexNodeSegments
    
    val uarr = Array(Bootstrap.BootstrapObjectAllocaterUUID)
    val iarr = Array(8192)
    val ilim = Array(20)
    val narr = Array(nodeSize)
    val clientUUID = new UUID(0,1)
    
    def loadFileSystem(kvos: KeyValueObjectState): Future[FileSystem] = kvos.contents.get(CumuloFSKey) match {
      case Some(arr) =>
        println("CumuloFS already created")
        SimpleFileSystem.load(sys, KeyValueObjectPointer(arr), clientUUID)
      
      case None =>
        println("Creating CumuloFS")
        implicit val tx = sys.newTransaction()
        for {
          alloc <- sys.getObjectAllocater(Bootstrap.BootstrapObjectAllocaterUUID)
      
          
          ptr <- FileSystem.prepareNewFileSystem(sys.radiclePointer, kvos.revision, alloc, Bootstrap.BootstrapObjectAllocaterUUID, uarr, iarr, ilim, uarr, iarr, uarr, narr,
                   Bootstrap.BootstrapObjectAllocaterUUID, fileSegmentSize)
          
          _=tx.update(sys.radiclePointer, None, Nil, Insert(CumuloFSKey, ptr.toArray) :: Nil)
                   
          txdone <- tx.commit()
          
          fs <- SimpleFileSystem.load(sys, ptr, clientUUID)
        } yield fs
    }
    
    sys.readObject(sys.radiclePointer).flatMap(loadFileSystem)
  }
  
  def cumulofs(log4jConfigFile: File, cfg: ConfigFile.Config): Unit = {
    
    setLog4jConfigFile(log4jConfigFile)
    
    val sys = createSystem(cfg)
    
    val f = initializeCumulofs(sys)
    
    val fs = Await.result(f, Duration(5000, MILLISECONDS))
    
    import com.ibm.aspen.fuse.protocol.Capabilities._
      
    val caps = FUSE_CAP_ASYNC_READ
  
    val ops = FuseOptions(
      max_readahead = 10,
      max_background = 10,
      congestion_threshold = 7,
      max_write = 16*1024,
      time_gran = 1,
      requestedCapabilities = caps)
      
    val channel = RemoteFuse.connect("127.0.0.1", 1111, None)
    
    val fi = new FuseInterface(fs, "/mnt", "cumulofs", 0, None, ops, channel, channel)
    fi.startHandlerDaemonThread()
    
    println(s"Initialized cumulofs")
    
    Thread.sleep(99999999)
  }
  
  def rebuild(storeName: String, cfg: ConfigFile.Config): Unit = {
    println(s"Rebuilding $storeName")
    
    val arr = storeName.split(":")
                
    val poolName = arr(0)
    val storeNumber = Integer.parseInt(arr(1))
    
    var o: Option[(ConfigFile.StorageNode, ConfigFile.DataStore)] = None
    
    cfg.nodes.values.foreach { n =>
      n.stores.foreach { s =>
        if (s.pool == poolName && s.store == storeNumber)
          o = Some((n, s))
      }
    }
    
    o match {
      case None => println(s"Store not found: $storeName")
      
      case Some((node, store)) =>
        val pool = cfg.pools(store.pool)
        
        val dataStoreId = DataStoreID(pool.uuid, store.store.asInstanceOf[Byte])
      
        
        val backend = store.backend match {
          case b: ConfigFile.RocksDB =>
            println(s"Creating data store $dataStoreId. Path ${b.path}")
            new RocksDBDataStoreBackend(b.path) 
        }
        
        val sys = createSystem(cfg)
        
        val pcomplete = Promise[Unit]()
        
        def doRebuild(iter: AllocatedObjectsIterator): Unit = {
          
          iter.fetchNext().foreach { o => o match {
            case None =>
              println(s"Done!!")
              pcomplete.success(())
            
            case Some(ao) => sys.readObject(ao.pointer) foreach { o => 
              val md = ObjectMetadata(o.revision, o.refcount, o.timestamp)
              val data = o.getRebuildDataForStore(dataStoreId).get
              val objectId = StoreObjectID(ao.pointer.uuid, ao.pointer.getStorePointer(dataStoreId).get)
              println(s"Restoring object $md with data length ${data.size}")
              //doRebuild(iter)
              backend.putObject(objectId, md, data).foreach(_ => doRebuild(iter))
            }
          }}
        }
        
        println(s"Beginning rebuild of $node, $store")
        
        val f = for {
          spool <- sys.getStoragePool(pool.uuid)
          _=println(s"Getting AllocatedObjectsIterator")
          iter <- spool.getAllocatedObjectsIterator()
          _=doRebuild(iter)
          _<-pcomplete.future
        } yield ()
        
        Await.result(f, Duration(5000, MILLISECONDS))   
    }
  }
  
  def node(nodeName: String, cfg: ConfigFile.Config): Unit = {
    
    val node = cfg.nodes.get(nodeName).getOrElse(throw new ConfigError(s"Invalid node name $nodeName"))
    
    setLog4jConfigFile(node.log4jConfigFile)
    
    val crl = node.crl match {
      case b: ConfigFile.RocksDB => 
        val crl = new RocksDBCrashRecoveryLog(b.path)
        val finit = crl.initialize()
        Await.result(finit, Duration(5000, MILLISECONDS))
        crl
    }
    
    val stores = node.stores.map { s =>
      val dataStoreId = DataStoreID(cfg.pools(s.pool).uuid, s.store.asInstanceOf[Byte])
      
      val backend = s.backend match {
        case b: ConfigFile.RocksDB =>
          println(s"Creating data store $dataStoreId. Path ${b.path}")
          new RocksDBDataStoreBackend(b.path) 
      }
      
      val txrs = crl.getTransactionRecoveryStateForStore(dataStoreId)
      val allocrs = crl.getAllocationRecoveryStateForStore(dataStoreId)
      
      (dataStoreId -> new DataStoreFrontend(dataStoreId, backend, txrs, allocrs))
    }.toMap
    
    val nnet = new NettyNetwork(cfg)
    
    val sys = createSystem(cfg, Some(nnet))
    
    val nodeNet = nnet.createStoreNetwork(nodeName)
    
    val storageNode = new StorageNode(sys, crl, nodeNet)
    
    object dsFactory extends DataStore.Factory {

      override def apply(
          storeId: DataStoreID,
          transactionRecoveryStates: List[TransactionRecoveryState],
          allocationRecoveryStates: List[AllocationRecoveryState]): Future[DataStore] = Future.successful(stores(storeId))
    
    }
    
    val f = Future.sequence(stores.keys.map( storeId => storageNode.addStore(storeId, dsFactory.apply)))
    
    Await.result(f, Duration(5000, MILLISECONDS))
    
    val faRegistry = BaseImplTypeRegistry(sys)
    
    val finalizerFactory = new BaseTransactionFinalizer(sys)
     
    val txHeartbeatPeriod = Duration(1, SECONDS)
    val txHeartbeatTimeout = Duration(3, SECONDS) // Delay until another store tries to drive transaction to completion
    val txRetryDelay = Duration(3, SECONDS) // Delay between advancing the Paxos round and sending new prepare messages
    
    val txDriverFactory = SimpleFixedDelayTransactionDriver.factory(txRetryDelay)
    
    def txcomplete(txuuid: UUID): Option[Boolean] = sys.transactionCache.getIfPresent(txuuid)

    val txMgr = new SimpleStorageNodeTxManager(txHeartbeatPeriod, txHeartbeatTimeout, storageNode.crl, txcomplete, storageNode.net.transactionHandler, 
                     txDriverFactory, finalizerFactory.factory)
    
    val allocHeartbeatPeriod   = Duration(3, SECONDS)
    val allocTimeout           = Duration(4, SECONDS)
    val allocStatusQueryPeriod = Duration(1, SECONDS)
    
    val allocMgr = new SimpleStorageNodeAllocationManager(allocHeartbeatPeriod, allocTimeout, allocStatusQueryPeriod, sys,
                       storageNode.crl, storageNode.net.allocationHandler)
    
    storageNode.recoverPendingOperations(txMgr, allocMgr)
    
    println(s"Initialized storage node: $nodeName")
    
    Thread.sleep(99999999)
  }
  
  def bootstrap(cfg: ConfigFile.Config): Unit = {

    if (cfg.oradicle.isDefined)
      throw new ConfigError("Radicle Pointer is defined. Bootstrap process is already complete!")
    
    val bootstrapStores = cfg.nodes.values.flatMap(n => n.stores).map { s =>
      
      val dataStoreId = DataStoreID(cfg.pools(s.pool).uuid, s.store.asInstanceOf[Byte])
      val backend = s.backend match {
        case b: ConfigFile.RocksDB =>
          println(s"Creating data store $dataStoreId. Path ${b.path}")
          new RocksDBDataStoreBackend(b.path) 
      }
      
      new DataStoreFrontend(dataStoreId, backend, Nil, Nil)
    }.toList
    
    val bootstrapPoolIDA = cfg.allocaters("bootstrap-allocater").ida
    
    val missedUpdateStrategy = cfg.pools("bootstrap-pool").missedUpdateStrategy match {
      case pss: ConfigFile.PerStoreSet => 
        PerStoreMissedUpdate.getStrategy(pss.allocaters.map(name => cfg.allocaters(name).uuid).toArray, pss.nodeSizes.toArray, pss.nodeLimits.toArray)
    }
    
    val fptr = Bootstrap.initializeNewSystem(bootstrapStores, bootstrapPoolIDA, missedUpdateStrategy)
    
    val radicle = Await.result(fptr, Duration(5000, MILLISECONDS))
    
    // Print yaml representation of Radicle Pointer
    println("# Radicle Pointer Definition")
    println("radicle:")
    println(s"    uuid:      ${radicle.uuid}")
    println(s"    pool-uuid: ${radicle.poolUUID}")
    radicle.size.foreach(size => println(s"    size:      ${size}"))
    println("    ida:")
    radicle.ida match {
      case ida: Replication => 
        println(s"        type:            replication")
        println(s"        width:           ${ida.width}")
        println(s"        write-threshold: ${ida.writeThreshold}")
        
      case ida: ReedSolomon => throw new NotImplementedError
    }
    println("    store-pointers:")
    radicle.storePointers.foreach { sp =>
      println(s"        - pool-index: ${sp.poolIndex}")
      if (sp.data.length > 0)
        println(s"          data: ${java.util.Base64.getEncoder.encodeToString(sp.data)}")
    }
  }
}