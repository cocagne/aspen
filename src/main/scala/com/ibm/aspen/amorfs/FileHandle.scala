package com.ibm.aspen.amorfs

import com.ibm.aspen.core.DataBuffer

import scala.concurrent.{ExecutionContext, Future}

trait FileHandle {
  val file: File
  
  def read(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[Option[DataBuffer]]  
  
  def write(offset: Long, buffers: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit]
  
  def write(offset: Long, buffer: DataBuffer)(implicit ec: ExecutionContext): Future[Unit] = write(offset, List(buffer))

  /** Outer future completes when the Inode and index have been updated to the new size. Inner future completes
    * when the background data deletion operation is done
    */
  def truncate(offset: Long)(implicit ec: ExecutionContext): Future[Future[Unit]]
  
  def flush()(implicit ec: ExecutionContext): Future[Unit]
}