package com.ibm.aspen.cumulofs.nfs

import com.ibm.aspen.cumulofs.BlockDevice
import scala.concurrent.ExecutionContext

class NFSBlockDevice(val file: BlockDevice)(implicit ec: ExecutionContext) extends NFSBaseFile {

  def rdev: Int = file.rdev

  def setrdev(newrdev: Int): Unit = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.setrdev(newrdev)
    }
  }
}
