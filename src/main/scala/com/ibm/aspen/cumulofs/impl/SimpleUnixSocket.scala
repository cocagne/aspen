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
    protected var cachedInode: UnixSocketInode,
    fs: FileSystem) extends SimpleBaseFile(fs) with UnixSocket {
  
  val pointer: UnixSocketPointer = synchronized { cachedInode.pointer }
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(cachedInode.pointer).map { refreshedInode => synchronized {
      cachedInode = refreshedInode
    }}
  }
  
  override def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Array[Byte]], newRefcount: Option[ObjectRefcount]): Unit = synchronized {
   cachedInode = new UnixSocketInode(cachedInode.pointer, newRevision, newRefcount.getOrElse(cachedInode.refcount), newTimestamp, updatedState)
  }
  
}