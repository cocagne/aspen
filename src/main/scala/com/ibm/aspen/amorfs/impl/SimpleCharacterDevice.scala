package com.ibm.aspen.amorfs.impl


import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.amorfs._

import scala.concurrent.{ExecutionContext, Future}

object SimpleCharacterDevice {
  case class SetDeviceType(rdev: Int) extends SimpleBaseFile.SimpleSet {
    def update(inode: Inode): Inode = inode.asInstanceOf[CharacterDeviceInode].setDeviceType(rdev)
  }
}

class SimpleCharacterDevice(override val pointer: CharacterDevicePointer,
                            initialInode: CharacterDeviceInode,
                            revision: ObjectRevision,
                            fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with CharacterDevice {
  
  import SimpleCharacterDevice._

  override def inode: CharacterDeviceInode = super.inode.asInstanceOf[CharacterDeviceInode]

  def rdev: Int = inode.rdev
  
  def setrdev(newrdev: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetDeviceType(rdev))
}