package com.ibm.aspen.amorfs.nfs

import com.ibm.aspen.amorfs.Symlink

import scala.concurrent.ExecutionContext

class NFSSymlink(val file: Symlink)(implicit ec: ExecutionContext) extends NFSBaseFile {

  def size: Int = file.size

  def symLink: Array[Byte] = file.symLink

  def setSymLink(newLink: Array[Byte]): Unit = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.setSymLink(newLink)
    }
  }

  def symLinkAsString: String = file.symLinkAsString
}
