package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.cumulofs._

import scala.concurrent.{ExecutionContext, Future}

object SimpleSymlink {
  case class SetSymLink(newLink: Array[Byte]) extends SimpleBaseFile.SimpleSet {
    def update(inode: Inode): Inode = inode.asInstanceOf[SymlinkInode].setContents(newLink)
  }
}

class SimpleSymlink(override val pointer: SymlinkPointer,
                    override protected var cachedInode: SymlinkInode,
                    revision: ObjectRevision,
                    fs: FileSystem) extends SimpleBaseFile(pointer, revision, cachedInode, fs) with Symlink {
  
  import SimpleSymlink._
  
  def size: Int = synchronized { cachedInode.content.length }
  
  def symLink: Array[Byte] = synchronized { cachedInode.content }
  
  def setSymLink(newLink: Array[Byte])(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetSymLink(newLink))
}