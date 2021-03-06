package com.ibm.aspen.amoeba

import com.ibm.aspen.core.DataBuffer

import scala.concurrent.{ExecutionContext, Future}

trait File extends BaseFile {
  val pointer: FilePointer
  
  def inode: FileInode

  def size: Long = inode.size

  def read(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[Option[DataBuffer]]

  def write(offset: Long,
            buffers: List[DataBuffer])(implicit ec: ExecutionContext): Future[(Long, List[DataBuffer])]

  /** Outer future completes when the Inode and index have been updated to the new size. Inner future completes
    * when the background data deletion operation is done
    */
  def truncate(offset: Long)(implicit ec: ExecutionContext): Future[Future[Unit]]

  def write(offset: Long,
            buffer: DataBuffer)(implicit ec: ExecutionContext): Future[(Long, List[DataBuffer])] = {
    write(offset, List(buffer))
  }

  def debugReadFully()(implicit ec: ExecutionContext): Future[Array[Byte]]

  private[this] var openHandles: Set[FileHandle] = Set()

  def open(): FileHandle = {
    val fh = fs.openFileHandle(this)
    synchronized { openHandles += fh }
    fh
  }

  private[amoeba] def close(fh: FileHandle): Unit =  {
    synchronized { openHandles -= fh }
    fs.closeFileHandle(this)
  }

  private[amoeba] def hasOpenHandles: Boolean = synchronized { openHandles.nonEmpty }

}