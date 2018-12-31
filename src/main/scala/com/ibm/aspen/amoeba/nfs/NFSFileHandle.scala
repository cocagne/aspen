package com.ibm.aspen.amoeba.nfs

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.amoeba.FileHandle

import scala.concurrent.ExecutionContext

class NFSFileHandle(fh: FileHandle)(implicit ec: ExecutionContext) {

  def size: Long = fh.file.size

  def read(offset: Long, nbytes: Int): Option[DataBuffer] = blockingCall(fh.read(offset, nbytes))

  def write(offset: Long, buffers: List[DataBuffer]): Int = {
    val nwritten = buffers.foldLeft(0)((sz, db) => sz + db.size)
    blockingCall(fh.write(offset, buffers))
    nwritten
  }

  def write(offset: Long, buffer: DataBuffer): Int = write(offset, List(buffer))

  def truncate(offset: Long): Unit = blockingCall(fh.truncate(offset))

  def flush(): Unit = blockingCall(fh.flush())
}
