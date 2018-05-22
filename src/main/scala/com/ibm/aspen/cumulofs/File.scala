package com.ibm.aspen.cumulofs

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.DataBuffer

trait File extends BaseFile {
  val pointer: FilePointer
  
  def size: Long
  
  def write(offset: Long, data: DataBuffer)(implicit ec: ExecutionContext): Future[Unit]
  def write(offset: Long, data: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit] 
  
  def append(data: DataBuffer)(implicit ec: ExecutionContext): Future[Unit]
  def append(data: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit]
  
  def debugRead()(implicit ec: ExecutionContext): Future[Array[Byte]]
}