package com.ibm.aspen.amoeba.impl


import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.amoeba._

import scala.concurrent.{ExecutionContext, Future}

object SimpleBlockDevice {
  case class SetDeviceType(rdev: Int) extends SimpleBaseFile.SimpleSet {
    def update(inode: Inode): Inode = inode.asInstanceOf[BlockDeviceInode].setDeviceType(rdev)
  }
}

class SimpleBlockDevice(override val pointer: BlockDevicePointer,
                        initialInode: BlockDeviceInode,
                        revision: ObjectRevision,
                        fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with BlockDevice {

  import SimpleBlockDevice._

  override def inode: BlockDeviceInode = super.inode.asInstanceOf[BlockDeviceInode]

  def rdev: Int = inode.rdev

  def setrdev(newrdev: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetDeviceType(rdev))
}
