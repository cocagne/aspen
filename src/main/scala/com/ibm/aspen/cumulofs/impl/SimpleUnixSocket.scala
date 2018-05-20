package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.UnixSocket
import com.ibm.aspen.cumulofs.UnixSocketInode
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.UnixSocketPointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.ObjectRefcount

class SimpleUnixSocket(
    protected var inode: UnixSocketInode,
    fs: FileSystem) extends SimpleBaseFile(fs) with UnixSocket {
  
  val pointer: UnixSocketPointer = inode.pointer
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(inode.pointer).map { refreshedInode => synchronized {
      inode = refreshedInode
    }}
  }
  
  override protected def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value], newRefcount: Option[ObjectRefcount]): Unit = {
   inode = new UnixSocketInode(inode.pointer, newRevision, newRefcount.getOrElse(inode.refcount), newTimestamp, updatedState)
  }
  
}