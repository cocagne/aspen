package com.ibm.aspen.amoeba.impl

import com.ibm.aspen.base.TestSystemSuite
import scala.concurrent.Future
import com.ibm.aspen.base.impl.Bootstrap
import java.util.UUID
import com.ibm.aspen.amoeba.FileInode
import com.ibm.aspen.amoeba.FileSystem

trait AmoebaBootstrap extends TestSystemSuite{
  
  def bootstrap(numIndexNodeSegments: Int = 5, fileSegmentSize:Int=4096): Future[FileSystem] = {
    implicit val tx = sys.newTransaction()
    
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
      
    } yield fs 
  }
  
}