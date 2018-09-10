package com.ibm.aspen.cumulofs.impl


import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.cumulofs._

import scala.concurrent.{ExecutionContext, Future}

object SimpleCharacterDevice {
  case class SetDeviceType(rdev: Int) extends SimpleBaseFile.SimpleSet {
    def update(inode: Inode): Inode = inode.asInstanceOf[CharacterDeviceInode].setDeviceType(rdev)
  }
}

class SimpleCharacterDevice(override val pointer: CharacterDevicePointer,
                            override protected var cachedInode: CharacterDeviceInode,
                            revision: ObjectRevision,
                            fs: FileSystem) extends SimpleBaseFile(pointer, revision, cachedInode, fs) with CharacterDevice {
  
  import SimpleCharacterDevice._

  def rdev: Int = { cachedInode.rdev }
  
  def setrdev(newrdev: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetDeviceType(rdev))
}