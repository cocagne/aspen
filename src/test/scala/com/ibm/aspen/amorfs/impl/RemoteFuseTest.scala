package com.ibm.aspen.amorfs.impl

import java.nio.charset.StandardCharsets
import java.util.UUID

import com.ibm.aspen.base.impl.{BasicAspenSystem, Bootstrap}
import com.ibm.aspen.base.{TestSystem, Transaction}
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.amorfs.FileSystem
import com.ibm.aspen.fuse.{FuseOptions, RemoteFuse}

import scala.concurrent.{ExecutionContext, Future}

object RemoteFuseTest {
  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global
  /*
  def bootstrap(sys: BasicAspenSystem, numIndexNodeSegments: Int = 5, fileSegmentSize:Int=4096): Future[FileSystem] = {
    implicit val tx: Transaction = sys.newTransaction()
    
    // Approximate the size of the node needed to store numSegments
    val nodeSize = (sys.radiclePointer.encodedSize + 2) * numIndexNodeSegments
    
    val uarr = Array(Bootstrap.BootstrapObjectAllocaterUUID)
    val iarr = Array(8192)
    val larr = Array(20)
    val narr = Array(nodeSize)
    val clientUUID = new UUID(0,1)

    for {
      r <- sys.readObject(sys.radiclePointer)
      
      // give transaction something to do
      _ = tx.bumpVersion(sys.radiclePointer, r.revision)
      
      alloc <- sys.getObjectAllocater(Bootstrap.BootstrapObjectAllocaterUUID)
      
      ptr <- FileSystem.prepareNewFileSystem(sys.radiclePointer, r.revision, alloc, Bootstrap.BootstrapObjectAllocaterUUID, uarr, iarr, larr, uarr, iarr, uarr, narr,
               Bootstrap.BootstrapObjectAllocaterUUID, fileSegmentSize)
      
      _ <- tx.commit()
      
      fs <- SimpleFileSystem.load(sys, ptr, clientUUID)
      
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("hello_world", mode=0x1A4, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      db = DataBuffer("Amorfs Lives!!!!\n".getBytes(StandardCharsets.UTF_8))
      _ <- new SimpleFileHandle(file, 0).write(0, db)
      
    } yield fs 
  }
  
  def main(args: Array[String]) {
    val ts = new TestSystem()
    var fi: FuseInterface = null
    
    println("Bootstrapping")
    bootstrap(ts.aspenSystem) foreach { fs =>
      println("Bootstrap complete")
      
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
      
      fi = new FuseInterface(fs, "/mnt", "amorfs", 0, None, ops, channel, channel)
      fi.startHandlerDaemonThread()
    }

    Thread.sleep(30000)
  }
  */
}