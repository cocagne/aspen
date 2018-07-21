package com.ibm.aspen.cumulofs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer

trait FileHandle {
  val file: File
  
  def read(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[Option[DataBuffer]]  
  
  def write(offset: Long, buffers: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit]
  
  def write(offset: Long, buffer: DataBuffer)(implicit ec: ExecutionContext): Future[Unit] = write(offset, List(buffer))
  
  def truncate(offset: Long)(implicit ec: ExecutionContext): Future[Unit]
  
  def flush()(implicit ec: ExecutionContext): Future[Unit]
}