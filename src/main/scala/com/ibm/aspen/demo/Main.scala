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
import com.ibm.aspen.core.network.StorageNodeID
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
import com.ibm.aspen.cumulofs.impl.SimpleFileSystem
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.fuse.FuseOptions
import com.ibm.aspen.fuse.RemoteFuse
import com.ibm.aspen.cumulofs.impl.FuseInterface
import com.ibm.aspen.base.impl.BaseImplTypeRegistry
import com.ibm.aspen.base.impl.BaseTransactionFinalizer
import com.ibm.aspen.base.impl.StorageNodeTransactionManager
import com.ibm.aspen.base.impl.StorageNodeAllocationManager
import com.ibm.aspen.core.transaction.TransactionDriver
import com.ibm.aspen.base.impl.SuperSimpleRetryingReadDriver
import com.ibm.aspen.cumulofs.CumuloFSTypeRegistry

object Main {
  
  val CumuloFSKey = Key("cumulofs")
  
  case class Args(mode:String="", configFile:File=null, nodeName:String="", host:String="", port:Int=0)
 
  class ConfigError(msg: String) extends Exception(msg)
  
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
            arg[File]("<config-file>").text("Configuration File").
              action( (x, c) => c.copy(configFile=x)).
              validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
              
            arg[String]("<host>").text("Storage Node Name").action((x,c) => c.copy(host=x)),
            
            arg[Int]("<port>").text("Storage Node Name").action((x,c) => c.copy(port=x))
        )
        
      checkConfig( c => if (c.mode == "") failure("Invalid command") else success )
    }
    
    parser.parse(args, Args()) match {
      case Some(cfg) =>
        println(s"Successful config: $cfg")
        try {
          val config = ConfigFile.loadConfig(cfg.configFile)
          println(s"Config file: $config")
          cfg.mode match {
            case "bootstrap" => bootstrap(config)
            case "node" => node(cfg.nodeName, config)
            case "cumulofs" => cumulofs(config)
          }
        } catch {
          case e: YamlFormat.FormatError => println(s"Error loading config file: $e")
          case e: ConfigError => println(s"Error: $e")
        }
      case None =>
    }
  }
  
  def initializeCumulofs(sys: BasicAspenSystem, numIndexNodeSegments: Int = 100, fileSegmentSize:Int=1024*1024): Future[FileSystem] = {
    
    // Approximate the size of the node needed to store numSegments
    val nodeSize = (sys.radiclePointer.encodedSize + 2) * numIndexNodeSegments
    
    val uarr = Array(Bootstrap.BootstrapObjectAllocaterUUID)
    val iarr = Array(8192)
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
      
          
          ptr <- FileSystem.prepareNewFileSystem(sys.radiclePointer, kvos.revision, alloc, Bootstrap.BootstrapObjectAllocaterUUID, uarr, iarr, uarr, iarr, uarr, narr,
                   Bootstrap.BootstrapObjectAllocaterUUID, fileSegmentSize)
          
          _=tx.append(sys.radiclePointer, None, Nil, KeyValueOperation.insertOperations(List((CumuloFSKey -> ptr.toArray))))
                   
          txdone <- tx.commit()
          
          fs <- SimpleFileSystem.load(sys, ptr, clientUUID)
        } yield fs
    }
    
    sys.readObject(sys.radiclePointer).flatMap(loadFileSystem)
  }
  
  def cumulofs(cfg: ConfigFile.Config): Unit = {
    
    val radiclePointer = cfg.oradicle.getOrElse(throw new ConfigError("Radicle Pointer is missing from the config file!"))
    
    val bootstrapPoolIDA = cfg.allocaters("bootstrap-allocater").ida

    val nnet = new NettyNetwork(cfg)
    val cliNet = nnet.createClientNetwork()
    
    val sys = new BasicAspenSystem(
        chooseDesignatedLeader = nnet.chooseDesignatedLeader _,
        isStorageNodeOnline = (_:StorageNodeID) => true,
        net = cliNet,
        defaultReadDriverFactory = SuperSimpleRetryingReadDriver.factory(ExecutionContext.Implicits.global) _,
        defaultTransactionDriverFactory = ClientTransactionDriver.noErrorRecoveryFactory,
        defaultAllocationDriverFactory = BaseAllocationDriver.NoErrorRecoveryAllocationDriver,
        transactionFactory = BaseTransaction.Factory,
        storagePoolFactory = BaseStoragePool.Factory,
        bootstrapPoolIDA = bootstrapPoolIDA,
        radiclePointer = radiclePointer,
        retryStrategy = new NoRetry,
        userTypeRegistry = Some(CumuloFSTypeRegistry)
        )
    
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
  
  def node(nodeName: String, cfg: ConfigFile.Config): Unit = {

    val radiclePointer = cfg.oradicle.getOrElse(throw new ConfigError("Radicle Pointer is missing from the config file!"))
    
    val node = cfg.nodes.get(nodeName).getOrElse(throw new ConfigError(s"Invalid node name $nodeName"))
    
    val crl = node.crl match {
      case b: ConfigFile.RocksDB => new RocksDBCrashRecoveryLog(b.path) 
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
    
    val bootstrapPoolIDA = cfg.allocaters("bootstrap-allocater").ida
    
    val clientId = ClientID(UUID.randomUUID())
    
    val nnet = new NettyNetwork(cfg)
    val nodeNet = nnet.createStoreNetwork(nodeName)
    val cliNet = nnet.createClientNetwork()
    //val znodeNet = new ZStoreNetwork(nodeName, cfg)
    //val zcliNet = new ZClientNetwork(clientId, cfg)
    
    val sys = new BasicAspenSystem(
        chooseDesignatedLeader = (o:ObjectPointer) => 0,
        isStorageNodeOnline = (_:StorageNodeID) => true,
        net = cliNet,
        defaultReadDriverFactory = SuperSimpleRetryingReadDriver.factory(ExecutionContext.Implicits.global) _,
        defaultTransactionDriverFactory = ClientTransactionDriver.noErrorRecoveryFactory,
        defaultAllocationDriverFactory = BaseAllocationDriver.NoErrorRecoveryAllocationDriver,
        transactionFactory = BaseTransaction.Factory,
        storagePoolFactory = BaseStoragePool.Factory,
        bootstrapPoolIDA = bootstrapPoolIDA,
        radiclePointer = radiclePointer,
        retryStrategy = new NoRetry,
        userTypeRegistry = None
        )
    
    val storageNode = new StorageNode(crl, nodeNet)
    
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
     
    val txMgr = new StorageNodeTransactionManager(storageNode.crl, storageNode.net.transactionHandler, TransactionDriver.noErrorRecoveryFactory, finalizerFactory.factory)
    val allocMgr = new StorageNodeAllocationManager(storageNode.crl, storageNode.net.allocationHandler)
    
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
    
    val fptr = Bootstrap.initializeNewSystem(bootstrapStores, bootstrapPoolIDA)
    
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