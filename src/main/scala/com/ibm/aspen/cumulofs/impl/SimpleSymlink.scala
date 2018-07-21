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
    protected var cachedInode: SymlinkInode,
    fs: FileSystem) extends SimpleBaseFile(fs) with Symlink {
  
  import SimpleSymlink._
  
  val pointer: SymlinkPointer = synchronized { cachedInode.pointer }
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(cachedInode.pointer).map { refreshedInode => synchronized {
      cachedInode = refreshedInode
    }}
  }
  
  override def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value], newRefcount: Option[ObjectRefcount]): Unit = synchronized {
   cachedInode = new SymlinkInode(cachedInode.pointer, newRevision, newRefcount.getOrElse(cachedInode.refcount), newTimestamp, updatedState)
  }
  
  def size: Int = synchronized { cachedInode.size }
  
  def link: String = synchronized { cachedInode.link }
  
  def setLink(newLink: String)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetLink(newLink))
}