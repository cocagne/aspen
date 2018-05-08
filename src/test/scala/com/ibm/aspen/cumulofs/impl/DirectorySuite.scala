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
import com.ibm.aspen.cumulofs.error.DirectoryNotEmpty
import com.ibm.aspen.core.read.InvalidObject
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.cumulofs.Timespec
import com.ibm.aspen.cumulofs.FileType
import com.ibm.aspen.cumulofs.FileMode

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
       rootDir <- fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
       rootInode <- rootDir.getInode()
     } yield {
       rootInode.uid should be (0)
     }
  }
  
  test("Create Directory") {
     for {
       fs <- bootstrap()
       oroot <- fs.inodeTable.lookup(0)
       rootDir <- fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
       initialContent <- rootDir.getContents()
       newDirPointer <- rootDir.createDirectory("foo", mode=0, uid=1, gid=2)
       newDir <- fs.loadDirectory(newDirPointer)
       newInode <- newDir.getInode()
       newContent <- rootDir.getContents()
     } yield {
       initialContent.length should be (0)
       newInode.uid should be (1)
       newInode.gid should be (2)
       newContent.length should be (1)
       newContent.head.name should be ("foo")
     }
  }
  
  test("Change Directory UID") {
     for {
       fs <- bootstrap()
       oroot <- fs.inodeTable.lookup(0)
       rootDir <- fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
       initialContent <- rootDir.getContents()
       newDirPointer <- rootDir.createDirectory("foo", mode=0, uid=1, gid=2)
       newDir <- fs.loadDirectory(newDirPointer)
       origUID = newDir.uid
       _ <- newDir.setUID(5)
       newDir2 <- fs.loadDirectory(newDirPointer)
     } yield {
       origUID should be (1)
       newDir.uid should be (5)
       newDir2.uid should be (5)
     }
  }
  
  test("Change Directory UID with recovery from revision mismatch") {
    def vbump(ptr: ObjectPointer, revision: ObjectRevision): Future[Unit] = {
      implicit val tx = sys.newTransaction()
      tx.bumpVersion(ptr, revision)
      tx.commit()
    }
    
    for {
      fs <- bootstrap()
      oroot <- fs.inodeTable.lookup(0)
      rootDir <- fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
      initialContent <- rootDir.getContents()
      newDirPointer <- rootDir.createDirectory("foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      origInode <- newDir.getInode()
      origUID = newDir.uid
      _ <- vbump(origInode.pointer.pointer, origInode.revision)
      _ <- newDir.setUID(5)
      newDir2 <- fs.loadDirectory(newDirPointer)
    } yield {
      origUID should be (1)
      newDir.uid should be (5)
      newDir2.uid should be (5)
    }
  }
  
  test("Change multiple metadata attributes") {
    val u = 6
    val g = 7
    val m = 1
    val ct = Timespec(1,2)
    val mt = Timespec(3,4)
    val at = Timespec(4,5)
    for {
      fs <- bootstrap()
      oroot <- fs.inodeTable.lookup(0)
      rootDir <- fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
      initialContent <- rootDir.getContents()
      newDirPointer <- rootDir.createDirectory("foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      fu = newDir.setUID(u)
      fg = newDir.setGID(g)
      fm = newDir.setMode(m)
      fc = newDir.setCtime(ct)
      fx = newDir.setMtime(mt)
      fa = newDir.setAtime(at)
      done <- Future.sequence(List(fu, fg, fm, fc, fx, fa))
      d <- fs.loadDirectory(newDirPointer)
    } yield {
      d.uid should be (u)
      d.gid should be (g)
      d.mode should be (m | FileMode.S_IFDIR)
      d.ctime should be (ct)
      d.mtime should be (mt)
      d.atime should be (at)
    }
  }
  
  test("Delete non-empty Directory") {
     for {
       fs <- bootstrap()
       oroot <- fs.inodeTable.lookup(0)
       rootDir <- fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
       initialContent <- rootDir.getContents()
       newDirPointer <- rootDir.createDirectory("foo", mode=0, uid=1, gid=2)
       newDir <- fs.loadDirectory(newDirPointer)
       newInode <- newDir.createDirectory("bar", mode=0, uid=1, gid=2)
       dc <- newDir.getContents()
       if (dc.length == 1)
       ruhRoh <- recoverToSucceededIf[DirectoryNotEmpty](rootDir.delete("foo"))
     } yield {
       initialContent.length should be (0)
     }
  }
  
  test("Delete empty Directory") {
     for {
       fs <- bootstrap()
       oroot <- fs.inodeTable.lookup(0)
       rootDir <- fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
       initialContent <- rootDir.getContents()
       newDirPointer <- rootDir.createDirectory("foo", mode=0, uid=1, gid=2)
       _ <- rootDir.delete("foo")       
       _ <- recoverToSucceededIf[InvalidObject](fs.inodeLoader.load(newDirPointer))
     } yield {
       initialContent.length should be (0)
     }
  }
  
  test("Delete Directory with data tiered list") {
     for {
       fs <- bootstrap()
       oroot <- fs.inodeTable.lookup(0)
       rootDir <- fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
       initialContent <- rootDir.getContents()
       newDirPointer <- rootDir.createDirectory("foo", mode=0, uid=1, gid=2)
       newDir <- fs.loadDirectory(newDirPointer)
       newInode <- newDir.createDirectory("bar", mode=0, uid=1, gid=2)
       dc <- newDir.getContents()
       if (dc.length == 1)
       _ <- newDir.delete("bar")       
       _ <- recoverToSucceededIf[InvalidObject](fs.inodeLoader.load(newInode))
       _ <- rootDir.delete("foo")       
       _ <- recoverToSucceededIf[InvalidObject](fs.inodeLoader.load(newDirPointer))
     } yield {
       initialContent.length should be (0)
     }
  }
}