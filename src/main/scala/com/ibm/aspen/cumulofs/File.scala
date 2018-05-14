package com.ibm.aspen.cumulofs

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.DataBuffer

trait File extends BaseFile {
  val pointer: FilePointer
  
  def size: Long
  
  def append(data: DataBuffer)(implicit ec: ExecutionContext): Future[Unit]
}