package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.FileHandle
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.FilePointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import java.nio.ByteBuffer

class SimpleFileHandle(
    val fs: FileSystem,
    val pointer: FilePointer) extends FileHandle {
 
  def size()(implicit ec: ExecutionContext): Future[Long] = ???
  def seek(offset: Long)(implicit ec: ExecutionContext): Future[Unit] = ???
  def tell()(implicit ec: ExecutionContext): Long = ???
  
  def read(size: Int, bb: ByteBuffer): Future[Unit] = ???
  def write(bb: ByteBuffer): Future[Unit] = ???
  
}