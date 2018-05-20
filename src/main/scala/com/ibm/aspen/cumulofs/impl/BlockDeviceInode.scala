package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.BlockDeviceInode
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.BlockDevice
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.cumulofs.Inode
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.cumulofs.BlockDevicePointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.cumulofs.DeviceInode
import com.ibm.aspen.core.objects.ObjectRefcount

object SimpleBlockDevice {
  case class SetDeviceType(rdev: Int) extends SimpleBaseFile.SimpleSet {
    def getUpdate(inode: Inode)(implicit tx: Transaction): (Key,Value) = DeviceInode.setDeviceType(rdev) 
  }
}

class SimpleBlockDevice(
    protected var inode: BlockDeviceInode,
    fs: FileSystem) extends SimpleBaseFile(fs) with BlockDevice {
  
  import SimpleBlockDevice._
  
  val pointer: BlockDevicePointer = inode.pointer
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(inode.pointer).map { refreshedInode => synchronized {
      inode = refreshedInode
    }}
  }
  
  override protected def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value], newRefcount: Option[ObjectRefcount]): Unit = {
   inode = new BlockDeviceInode(inode.pointer, newRevision, newRefcount.getOrElse(inode.refcount), newTimestamp, updatedState)
  }
  
  def rdev: Int = { inode.rdev }
  
  def setrdev(newrdev: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetDeviceType(rdev))
   
}