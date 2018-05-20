package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.FIFO
import com.ibm.aspen.cumulofs.FIFOInode
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.FIFOPointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.ObjectRefcount

class SimpleFIFO(
    protected var inode: FIFOInode,
    fs: FileSystem) extends SimpleBaseFile(fs) with FIFO {
  
  val pointer: FIFOPointer = inode.pointer
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(inode.pointer).map { refreshedInode => synchronized {
      inode = refreshedInode
    }}
  }
  
  override protected def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value], newRefcount: Option[ObjectRefcount]): Unit = {
   inode = new FIFOInode(inode.pointer, newRevision, newRefcount.getOrElse(inode.refcount), newTimestamp, updatedState)
  }
  
}