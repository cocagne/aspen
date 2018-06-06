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

object Main {
  
  
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
          }
        } catch {
          case e: YamlFormat.FormatError => println(s"Error loading config file: $e")
          case e: ConfigError => println(s"Error: $e")
        }
      case None =>
    }
  }
  
  def bootstrap(cfg: ConfigFile.Config): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    
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