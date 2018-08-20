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
    protected var cachedInode: FIFOInode,
    fs: FileSystem) extends SimpleBaseFile(fs) with FIFO {
  
  val pointer: FIFOPointer = synchronized { cachedInode.pointer }
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(cachedInode.pointer).map { refreshedInode => synchronized {
      cachedInode = refreshedInode
    }}
  }
  
  override def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Array[Byte]], newRefcount: Option[ObjectRefcount]): Unit = synchronized {
   cachedInode = new FIFOInode(cachedInode.pointer, newRevision, newRefcount.getOrElse(cachedInode.refcount), newTimestamp, updatedState)
  }
  
}