package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.base.TestSystem
import com.ibm.aspen.base.AspenSystem
import scala.concurrent.Future
import com.ibm.aspen.cumulofs.FileSystem
import scala.concurrent.ExecutionContext
import java.util.UUID
import com.ibm.aspen.cumulofs.FileInode
import com.ibm.aspen.base.impl.Bootstrap
import com.ibm.aspen.base.impl.BasicAspenSystem
import com.ibm.aspen.core.DataBuffer
import java.nio.charset.StandardCharsets
import com.ibm.aspen.fuse.FuseOptions
import com.ibm.aspen.fuse.RemoteFuse

object RemoteFuseTest {
  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global
  
  def bootstrap(sys: BasicAspenSystem, numIndexNodeSegments: Int = 5, fileSegmentSize:Int=4096): Future[FileSystem] = {
    implicit val tx = sys.newTransaction()
    
    // Approximate the size of the node needed to store numSegments
    val nodeSize = (sys.radiclePointer.encodedSize + 2) * numIndexNodeSegments
    
    val uarr = Array(Bootstrap.BootstrapObjectAllocaterUUID)
    val iarr = Array(8192)
    val narr = Array(nodeSize)
    val clientUUID = new UUID(0,1)
    
    val (iops, _) = FileInode.getInitialContent(0, 0, 0)
    
    for {
      r <- sys.readObject(sys.radiclePointer)
      
      // give transaction something to do
      meh = tx.bumpVersion(sys.radiclePointer, r.revision)
      
      alloc <- sys.getObjectAllocater(Bootstrap.BootstrapObjectAllocaterUUID)
      
      ptr <- FileSystem.prepareNewFileSystem(sys.radiclePointer, r.revision, alloc, Bootstrap.BootstrapObjectAllocaterUUID, uarr, iarr, uarr, iarr, uarr, narr,
               Bootstrap.BootstrapObjectAllocaterUUID, fileSegmentSize)
      
      txdone <- tx.commit()
      
      fs <- SimpleFileSystem.load(sys, ptr, clientUUID)
      
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("hello_world", mode=0x1A4, uid=1, gid=2)
      file <- fs.loadFile(newFilePointer)
      db = DataBuffer("CumuloFS Lives!!!!\n".getBytes(StandardCharsets.UTF_8))
      _ <- file.append(db)
      
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
      
      fi = new FuseInterface(fs, "/mnt", "cumulofs", 0, None, ops, channel, channel)
      fi.startHandlerDaemonThread()
    }

    Thread.sleep(30000)
  }
}