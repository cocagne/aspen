package com.ibm.aspen.amorfs.nfs

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.amorfs.File
import org.dcache.nfs.vfs.Stat

import scala.concurrent.ExecutionContext

class NFSFile(val file: File)(implicit ec: ExecutionContext) extends NFSBaseFile {

  def size: Long = file.size

  def read(offset: Long, nbytes: Int): Option[DataBuffer] = blockingCall(file.read(offset, nbytes))

  def truncate(offset: Long): Unit = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.truncate(offset)
    }
  }

  def debugReadFully(): Array[Byte] = blockingCall(file.debugReadFully())

  override def nfsStat: Stat = {
    val stats: Stat = super.nfsStat

    stats.setSize(size)

    stats
  }
}
