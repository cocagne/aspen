package com.ibm.aspen.cumulofs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import java.nio.ByteBuffer

trait FileHandle {
  val pointer: FilePointer
  val fs: FileSystem
  
  def getInode()(implicit ec: ExecutionContext): Future[FileInode] = {
    fs.inodeLoader.load(pointer) map ( _.asInstanceOf[FileInode] )
  }
  
  def size()(implicit ec: ExecutionContext): Future[Long]
  def seek(offset: Long)(implicit ec: ExecutionContext): Future[Unit]
  def tell()(implicit ec: ExecutionContext): Long
  
  def read(size: Int, bb: ByteBuffer): Future[Unit]
  def write(bb: ByteBuffer): Future[Unit]
}