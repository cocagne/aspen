package com.ibm.aspen.cumulofs.impl

import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest._
import com.ibm.aspen.base.TestSystemSuite
import com.ibm.aspen.base.impl.Bootstrap
import com.ibm.aspen.cumulofs.FileSystem
import org.scalactic.source.Position.apply
import com.ibm.aspen.cumulofs.DirectoryPointer
import com.ibm.aspen.base.task.LocalTaskGroup
import com.ibm.aspen.base.task.TaskGroupPointer
import java.util.UUID

class DirectorySuite extends TestSystemSuite {
  
  def bootstrap(): Future[FileSystem] = {
    implicit val tx = sys.newTransaction()
    val uarr = Array(Bootstrap.BootstrapObjectAllocaterUUID)
    val iarr = Array(8192)
    val clientUUID = new UUID(0,1)
    for {
      r <- sys.readObject(sys.radiclePointer)
      
      // give transaction something to do
      meh = tx.bumpVersion(sys.radiclePointer, r.revision)
      
      alloc <- sys.getObjectAllocater(Bootstrap.BootstrapObjectAllocaterUUID)
      ptr <- FileSystem.prepareNewFileSystem(sys.radiclePointer, r.revision, alloc, Bootstrap.BootstrapObjectAllocaterUUID, uarr, iarr, uarr, iarr, uarr, iarr)
      
      txdone <- tx.commit()
      
      fs <- SimpleFileSystem.load(sys, ptr, clientUUID)
      
    } yield fs 
  }
  
  test("CumuloFS Bootstrap") {
     for {
       fs <- bootstrap()
       oroot <- fs.inodeTable.lookup(0)
       rootDir = fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
       rootInode <- rootDir.getInode()
     } yield {
       rootInode.uid should be (0)
     }
  }
}