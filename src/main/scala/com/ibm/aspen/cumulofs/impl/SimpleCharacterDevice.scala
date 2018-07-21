package com.ibm.aspen.cumulofs.impl


import com.ibm.aspen.cumulofs.CharacterDeviceInode
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.CharacterDevice
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.cumulofs.Inode
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.cumulofs.CharacterDevicePointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.cumulofs.DeviceInode
import com.ibm.aspen.core.objects.ObjectRefcount

object SimpleCharacterDevice {
  case class SetDeviceType(rdev: Int) extends SimpleBaseFile.SimpleSet {
    def getUpdate(inode: Inode)(implicit tx: Transaction): (Key,Value) = DeviceInode.setDeviceType(rdev) 
  }
}

class SimpleCharacterDevice(
    protected var cachedInode: CharacterDeviceInode,
    fs: FileSystem) extends SimpleBaseFile(fs) with CharacterDevice {
  
  import SimpleCharacterDevice._
  
  val pointer: CharacterDevicePointer = synchronized { cachedInode.pointer }
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.load(cachedInode.pointer).map { refreshedInode => synchronized {
      cachedInode = refreshedInode
    }}
  }
  
  override def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value], newRefcount: Option[ObjectRefcount]): Unit = synchronized {
   cachedInode = new CharacterDeviceInode(cachedInode.pointer, newRevision, newRefcount.getOrElse(cachedInode.refcount), newTimestamp, updatedState)
  }
  
  def rdev: Int = { cachedInode.rdev }
  
  def setrdev(newrdev: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetDeviceType(rdev))
   
}