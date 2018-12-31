package com.ibm.aspen.demo

import java.io.{File, StringReader}
import java.util.UUID
import java.util.concurrent.Executors

import com.ibm.aspen.base._
import com.ibm.aspen.base.impl._
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.data_store._
import com.ibm.aspen.core.ida.{ReedSolomon, Replication}
import com.ibm.aspen.core.objects.keyvalue.{Insert, Key}
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, KeyValueObjectState}
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.amoeba.FileSystem
import com.ibm.aspen.amoeba.impl.{AmoebaTypeRegistry, FuseInterface, SimpleFileSystem}
import com.ibm.aspen.amoeba.nfs.AmoebaNFS
import com.ibm.aspen.fuse.{FuseOptions, RemoteFuse}
import org.dcache.nfs.ExportFile
import org.dcache.nfs.v3.xdr.{mount_prot, nfs3_prot}
import org.dcache.nfs.v3.{MountServer, NfsServerV3}
import org.dcache.nfs.v4.xdr.nfs4_prot
import org.dcache.nfs.v4.{MDSOperationFactory, NFSServerV41}
import org.dcache.nfs.vfs.VirtualFileSystem
import org.dcache.oncrpc4j.rpc.{OncRpcProgram, OncRpcSvcBuilder}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object Main {

  val AmoebafsKey = Key("amoeba")

  case class Args(mode:String="",
                  configFile:File=null,
                  log4jConfigFile: File=null,
                  nodeName:String="",
                  host:String="",
                  port:Int=0)

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

      cmd("amoeba").text("Launches an Amoeba server").
        action( (_,c) => c.copy(mode="amoeba")).
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

      cmd("amoeba-nfs").text("Launches a Amoeba NFS server").
        action( (_,c) => c.copy(mode="amoeba-nfs")).
        children(
          arg[File]("<config-file>").text("Aspen Configuration File").
            action( (x, c) => c.copy(configFile=x)).
            validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[File]("<log4j-config-file>").text("Log4j Configuration File").
            action( (x, c) => c.copy(log4jConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Log4j Config file does not exist: $x")),
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
                    case _: Throwable => failure("Store name must match the format \"pool-name:storeNumber\"")
                  }
                }
                else failure("Store name must match the format \"pool-name:storeNumber\"")
              }
       )

      checkConfig( c => if (c.mode == "") failure("Invalid command") else success )
    }

    parser.parse(args, Args()) match {
      case Some(cfg) =>
       //
        try {
          val config = ConfigFile.loadConfig(cfg.configFile)
          println(s"Successful config: $cfg")
          //println(s"Config file: $config")
          cfg.mode match {
            case "bootstrap" => bootstrap(config)
            case "node" => node(cfg.nodeName, config)
            case "amoeba" => amoeba_fuse(cfg.log4jConfigFile, config)
            case "amoeba-nfs" => amoeba_nfs(cfg.log4jConfigFile, config)
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
        val typeUUID: UUID = a.uuid
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
            case _: ClassCastException => None
          }
      }
    }

    val typeRegistry = new AggregateTypeRegistry(AmoebaTypeRegistry :: allocaterRegistry :: Nil)

    new BasicAspenSystem(
        chooseDesignatedLeader = cliNet.onlineTracker.chooseDesignatedLeader,
        getStorageHostFn = cliNet.onlineTracker.getStorageHostForStore,
        net = cliNet,
        defaultReadDriverFactory = SuperSimpleRetryingReadDriver.factory(opportunisticRebuildDelay, ExecutionContext.Implicits.global),
        defaultTransactionDriverFactory = SimpleClientTransactionDriver.factory(prepareRetransmitDelay),
        defaultAllocationDriverFactory = SuperSimpleRetryingAllocationDriver.factory(allocationRetransmitDelay),
        transactionFactory = BaseTransaction.Factory,
        storagePoolFactory = BaseStoragePool.Factory,
        bootstrapPoolIDA = bootstrapPoolIDA,
        radiclePointer = radiclePointer,
        retryStrategy = new ExponentialBackoffRetryStrategy(backoffLimit = 10, initialRetryDelay = 1),
        userTypeRegistry = Some(typeRegistry),
        otransactionCache = None
        )
  }

  def initializeAmoeba(sys: BasicAspenSystem, numIndexNodeSegments: Int = 100, fileSegmentSize:Int=1024*1024): Future[FileSystem] = {

    // Approximate the size of the node needed to store numSegments
    val nodeSize = (sys.radiclePointer.encodedSize + 2) * numIndexNodeSegments

    val uarr: Array[UUID] = Array(Bootstrap.BootstrapObjectAllocaterUUID)
    val iarr = Array(8192)
    val ilim = Array(20)
    val narr = Array(nodeSize)
    val clientUUID = new UUID(0,1)

    def loadFileSystem(kvos: KeyValueObjectState): Future[FileSystem] = kvos.contents.get(AmoebafsKey) match {
      case Some(arr) =>
        println("Amoeba already created")
        SimpleFileSystem.load(sys, KeyValueObjectPointer(arr), clientUUID)

      case None =>
        println("Creating Amoeba")
        implicit val tx: Transaction = sys.newTransaction()
        for {
          alloc <- sys.getObjectAllocater(Bootstrap.BootstrapObjectAllocaterUUID)


          ptr <- FileSystem.prepareNewFileSystem(
            sys.radiclePointer,
            kvos.revision,
            alloc,
            Bootstrap.BootstrapObjectAllocaterUUID,
            uarr, iarr, ilim, uarr, iarr, uarr, narr,
            Bootstrap.BootstrapObjectAllocaterUUID,
            fileSegmentSize)

          _=tx.update(sys.radiclePointer, None, Nil, Insert(AmoebafsKey, ptr.toArray) :: Nil)

          _ <- tx.commit()

          fs <- SimpleFileSystem.load(sys, ptr, clientUUID)
        } yield fs
    }

    sys.readObject(sys.radiclePointer).flatMap(loadFileSystem)
  }

  def amoeba_nfs(log4jConfigFile: File, cfg: ConfigFile.Config): Unit = {
    setLog4jConfigFile(log4jConfigFile)

    val sys = createSystem(cfg)

    val f = initializeAmoeba(sys)

    val fs = Await.result(f, Duration(5000, MILLISECONDS))

    val exports = "/ 192.168.56.2(rw)\n"

    val sched = Executors.newScheduledThreadPool(10)
    val ec = ExecutionContext.fromExecutorService(sched)

    val vfs: VirtualFileSystem = new AmoebaNFS(fs, ec)

    val nfsSvc = new OncRpcSvcBuilder().
      withPort(2049).
      withTCP.
      withAutoPublish.
      withWorkerThreadIoStrategy.
      build

    val exportFile = new ExportFile(new StringReader(exports))

    val nfs4 = new NFSServerV41.Builder().
      withExportFile(exportFile).
      withVfs(vfs).
      withOperationFactory(new MDSOperationFactory).
      build

    val nfs3 = new NfsServerV3(exportFile, vfs)
    val mountd = new MountServer(exportFile, vfs)

    //val portmapSvc = new OncRpcEmbeddedPortmap()

    nfsSvc.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), mountd)
    nfsSvc.register(new OncRpcProgram(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3), nfs3)
    nfsSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4)
    nfsSvc.start()

    println("Amoeba NFS server started...")
    Thread.currentThread.join()
  }

  def amoeba_fuse(log4jConfigFile: File, cfg: ConfigFile.Config): Unit = {

    setLog4jConfigFile(log4jConfigFile)

    val sys = createSystem(cfg)

    val f = initializeAmoeba(sys)
    
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
    
    val fi = new FuseInterface(fs, "/mnt", "amoeba", 0, None, ops, channel, channel)
    fi.startHandlerDaemonThread()
    
    println(s"Initialized Amoeba File System")
    
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
          
          iter.fetchNext().foreach {
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
          }
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
    
    val node = cfg.nodes.getOrElse(nodeName, throw new ConfigError(s"Invalid node name $nodeName"))
    
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
      
      dataStoreId -> new DataStoreFrontend(dataStoreId, backend, txrs, allocrs)
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

    val finalizerFactory = new BaseTransactionFinalizer(sys)
     
    val txHeartbeatPeriod = Duration(1, SECONDS)
    val txHeartbeatTimeout = Duration(3, SECONDS) // Delay until another store tries to drive transaction to completion
    val txRetryDelay = Duration(100, MILLISECONDS) //
    val txRetryCap = Duration(3, SECONDS)
    
    val txDriverFactory = SimpleStoreTransactionDriver.factory(initialDelay=txRetryDelay, maxDelay=txRetryCap)

    val txMgr = new SimpleStorageNodeTxManager(txHeartbeatPeriod, txHeartbeatTimeout, storageNode.crl, sys.transactionCache, storageNode.net.transactionHandler,
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
        val nodeLimits = pss.nodeLimits match {
          case None => Array(100)
          case Some(l) => l.toArray
        }
        PerStoreMissedUpdate.getStrategy(pss.allocaters.map(name => cfg.allocaters(name).uuid).toArray, pss.nodeSizes.toArray, nodeLimits)
    }
    
    val fptr = Bootstrap.initializeNewSystem(bootstrapStores, bootstrapPoolIDA, missedUpdateStrategy)
    
    val radicle = Await.result(fptr, Duration(5000, MILLISECONDS))
    
    // Print yaml representation of Radicle Pointer
    println("# Radicle Pointer Definition")
    println("radicle:")
    println(s"    uuid:      ${radicle.uuid}")
    println(s"    pool-uuid: ${radicle.poolUUID}")
    radicle.size.foreach(size => println(s"    size:      $size"))
    println("    ida:")
    radicle.ida match {
      case ida: Replication => 
        println(s"        type:            replication")
        println(s"        width:           ${ida.width}")
        println(s"        write-threshold: ${ida.writeThreshold}")
        
      case _: ReedSolomon => throw new NotImplementedError
    }
    println("    store-pointers:")
    radicle.storePointers.foreach { sp =>
      println(s"        - pool-index: ${sp.poolIndex}")
      if (sp.data.length > 0)
        println(s"          data: ${java.util.Base64.getEncoder.encodeToString(sp.data)}")
    }
  }
}