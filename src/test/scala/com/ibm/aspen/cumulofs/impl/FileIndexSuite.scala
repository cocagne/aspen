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
import com.ibm.aspen.core.read.InvalidObject
import com.ibm.aspen.cumulofs.FileInode
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.DataObjectPointer


class FileIndexSuite extends TestSystemSuite  {
  
  def bootstrap(numSegments: Int): Future[FileSystem] = {
    implicit val tx = sys.newTransaction()
    
    // Approximate the size of the node needed to store numSegments
    val nodeSize = (sys.radiclePointer.encodedSize + 2) * numSegments
    
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
      
      ptr <- FileSystem.prepareNewFileSystem(sys.radiclePointer, r.revision, alloc, Bootstrap.BootstrapObjectAllocaterUUID, uarr, iarr, uarr, iarr, uarr, narr)
      
      txdone <- tx.commit()
      
      fs <- SimpleFileSystem.load(sys, ptr, clientUUID)
      
    } yield fs 
  }
  
  def allocDataObject(): Future[DataObjectPointer] = {
    implicit val tx = sys.newTransaction()
    
    for {
      r <- sys.readObject(sys.radiclePointer)
      
      // give transaction something to do
      meh = tx.bumpVersion(sys.radiclePointer, r.revision)
      
      alloc <- sys.getObjectAllocater(Bootstrap.BootstrapObjectAllocaterUUID)
      
      ptr <- alloc.allocateDataObject(sys.radiclePointer, r.revision, DataBuffer.Empty)

      txdone <- tx.commit()
      
    } yield ptr 
  }
  
  test("Create file") {
    for {
      fs <- bootstrap(5)
      oroot <- fs.inodeTable.lookup(0)
      rootDir = fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      newInode <- fs.inodeLoader.load(newFilePointer)
      newContent <- rootDir.getContents()
    } yield {
      initialContent.length should be (0)
      newInode.uid should be (1)
      newInode.gid should be (2)
      newContent.length should be (1)
      newContent.head.name should be ("foo")
    }
  }
  
  test("Create single node file index") {
    implicit val tx = sys.newTransaction()
    
    for {
      fs <- bootstrap(5)
      oroot <- fs.inodeTable.lookup(0)
      rootDir = fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      origInode <- fs.inodeLoader.load(newFilePointer)
      dop <- allocDataObject()
      idx = new FileIndex(fs, FileIndex.NoCache, origInode)
      (root, fupdated) <- idx.prepareAppend(newFilePointer.pointer, origInode.revision, origInode.timestamp, 0, List((dop, 5)))
      done <- tx.commit()
      stateUpdated <- fupdated
      orootNode <- idx.getIndexNodeForOffset(0)
      orootNode2 <- idx.getIndexNodeForOffset(1)
    } yield {
      root.tails.length should be (1)
      root.tier0head should be (root.tails(0))
      orootNode match {
        case None => fail("Should be set!")
        case Some(rootNode) =>
          rootNode.leftPointer.isEmpty should be (true)
          rootNode.rightPointer.isEmpty should be (true)
          rootNode.segments.length should be (1)
          rootNode.revision should be (tx.txRevision)
          rootNode.timestamp.compareTo(origInode.timestamp) > 0 should be (true)
      }
      orootNode2 match {
        case None => fail("Should be set!")
        case Some(rootNode) =>
          rootNode.leftPointer.isEmpty should be (true)
          rootNode.rightPointer.isEmpty should be (true)
          rootNode.segments.length should be (1)
          rootNode.revision should be (tx.txRevision)
          rootNode.timestamp.compareTo(origInode.timestamp) > 0 should be (true)
      }
    }
  }
  
  test("Simple append") {
    
    def init() = {
      implicit val tx = sys.newTransaction()
      
      for {
        fs <- bootstrap(5)
        oroot <- fs.inodeTable.lookup(0)
        rootDir = fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
        initialContent <- rootDir.getContents()
        newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
        origInode <- fs.inodeLoader.load(newFilePointer)
        dop <- allocDataObject()
        idx = new FileIndex(fs, FileIndex.NoCache, origInode)
        (root, fupdated) <- idx.prepareAppend(newFilePointer.pointer, origInode.revision, origInode.timestamp, 0, List((dop, 5)))
        done <- tx.commit()
        stateUpdated <- fupdated  
      } yield (fs, idx, newFilePointer.pointer, tx.txRevision, tx.timestamp(), dop)
    }
    
    implicit val tx = sys.newTransaction()
    
    for {
      (fs, idx, fpointer, rev, ts, dop) <- init()
      (root, fupdated) <- idx.prepareAppend(fpointer, rev, ts, 10, List((dop, 6)))
      done <- tx.commit()
      stateUpdated <- fupdated
      orootNode <- idx.getIndexNodeForOffset(0)
    } yield {
      root.tails.length should be (1)
      root.tier0head should be (root.tails(0))
      orootNode match {
        case None => fail("Should be set!")
        case Some(rootNode) =>
          rootNode.leftPointer.isEmpty should be (true)
          rootNode.rightPointer.isEmpty should be (true)
          rootNode.segments.length should be (2)
          rootNode.revision should be (tx.txRevision)
          rootNode.headOffset should be (0)
          rootNode.tailOffset should be (10)
      }
    }
  }
  
  test("Create multi-node file index") {
    implicit val tx = sys.newTransaction()
    
    for {
      fs <- bootstrap(5)
      oroot <- fs.inodeTable.lookup(0)
      rootDir = fs.loadDirectory(oroot.get.asInstanceOf[DirectoryPointer])
      initialContent <- rootDir.getContents()
      newFilePointer <- rootDir.createFile("foo", mode=0, uid=1, gid=2)
      origInode <- fs.inodeLoader.load(newFilePointer)
      dop <- allocDataObject()
      idx = new FileIndex(fs, FileIndex.NoCache, origInode)
      segments = List((dop, 1), (dop,2), (dop,3), (dop,4), (dop,5), (dop,6))
      (root, fupdated) <- idx.prepareAppend(newFilePointer.pointer, origInode.revision, origInode.timestamp, 0, segments)
      done <- tx.commit()
      stateUpdated <- fupdated
      orootNode <- idx.getIndexNodeForOffset(0)
      otailNode <- idx.getIndexNodeForOffset(99999)
    } yield {
      root.tails.length should be (2)
      root.tier0head shouldNot be (root.tails(0))
      root.rootNode should be (root.tails(1))
      orootNode match {
        case None => fail("Should be set!")
        case Some(rootNode) =>
          rootNode.leftPointer.isEmpty should be (true)
          rootNode.rightPointer.isDefined should be (true)
          rootNode.segments.length should be (4)
          rootNode.revision should be (tx.txRevision)
          rootNode.timestamp.compareTo(origInode.timestamp) > 0 should be (true)
          rootNode.headOffset should be (0)
          rootNode.tailOffset should be (6)
          rootNode.rightPointer.get.offset should be (10)
      }
      otailNode match {
        case None => fail("Should be set!")
        case Some(tailNode) =>
          tailNode.leftPointer.isDefined should be (true)
          tailNode.rightPointer.isEmpty should be (true)
          tailNode.segments.length should be (2)
          tailNode.revision should be (tx.txRevision)
          tailNode.timestamp.compareTo(origInode.timestamp) > 0 should be (true)
          tailNode.headOffset should be (10)
          tailNode.tailOffset should be (15)
          tailNode.leftPointer.get.offset should be (0)
      }
    }
  }
  
}