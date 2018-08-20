package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.FileInode
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.FilePointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.base.ObjectAllocater
import scala.concurrent.Promise
import com.ibm.aspen.cumulofs.Inode
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.util.Varint
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.cumulofs.File
import com.ibm.aspen.cumulofs.Timespec
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.base.StopRetrying
import com.ibm.aspen.cumulofs.error.HolesNotSupported
import com.ibm.aspen.core.objects.ObjectRefcount

class SimpleFile(
    fs: FileSystem,
    protected var cachedInode: FileInode) extends SimpleBaseFile(fs) with File {
  
  val pointer: FilePointer = cachedInode.pointer
  
  def inode: FileInode = synchronized { cachedInode }
  
  override def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Array[Byte]], newRefcount: Option[ObjectRefcount]): Unit = synchronized {
    cachedInode = new FileInode(cachedInode.pointer, newRevision, newRefcount.getOrElse(cachedInode.refcount), newTimestamp, updatedState)
  }
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(cachedInode.pointer).map { refreshedInode => synchronized {
      cachedInode = refreshedInode
    }}
  }
  
  override def freeResources()(implicit ec: ExecutionContext): Future[Unit] = {
    new SimpleFileHandle(this, 0).truncate(0)
  }

}