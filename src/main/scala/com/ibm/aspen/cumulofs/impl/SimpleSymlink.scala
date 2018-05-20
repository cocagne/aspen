package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.SymlinkInode
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.Symlink
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.cumulofs.Inode
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.cumulofs.SymlinkPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectRefcount

object SimpleSymlink {
  case class SetLink(newLink: String) extends SimpleBaseFile.SimpleSet {
    def getUpdate(inode: Inode)(implicit tx: Transaction): (Key,Value) = SymlinkInode.setLink(newLink) 
  }
}

class SimpleSymlink(
    protected var inode: SymlinkInode,
    fs: FileSystem) extends SimpleBaseFile(fs) with Symlink {
  
  import SimpleSymlink._
  
  val pointer: SymlinkPointer = inode.pointer
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(inode.pointer).map { refreshedInode => synchronized {
      inode = refreshedInode
    }}
  }
  
  override protected def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value], newRefcount: Option[ObjectRefcount]): Unit = {
   inode = new SymlinkInode(inode.pointer, newRevision, newRefcount.getOrElse(inode.refcount), newTimestamp, updatedState)
  }
  
  def size: Int = synchronized { inode.size }
  
  def link: String = synchronized { inode.link }
  
  def setLink(newLink: String)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetLink(newLink))
}