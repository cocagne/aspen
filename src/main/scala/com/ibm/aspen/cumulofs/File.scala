package com.ibm.aspen.cumulofs

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.DataBuffer

trait File extends BaseFile {
  val pointer: FilePointer
  
  def currentInode: FileInode
  
  def refreshInode(newInode: Option[FileInode]=None)(implicit ec: ExecutionContext): Future[FileInode]
  
  def size: Long = currentInode.size
  
  def write(offset: Long, data: DataBuffer)(implicit ec: ExecutionContext): Future[Unit]
  def write(offset: Long, data: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit] 
  
  def append(data: DataBuffer)(implicit ec: ExecutionContext): Future[Unit]
  def append(data: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit]
  
  def read(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[Option[DataBuffer]]
  
  def truncate(offset: Long)(implicit ec: ExecutionContext): Future[Unit]
  
  def debugRead()(implicit ec: ExecutionContext): Future[Array[Byte]]
}