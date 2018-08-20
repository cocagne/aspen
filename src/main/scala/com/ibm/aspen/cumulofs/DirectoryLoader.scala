package com.ibm.aspen.cumulofs

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait DirectoryLoader {
  
  def directoryTableAllocaters: Array[UUID]
  def directoryTableSizes: Array[Int]
  def directoryTableKVPairLimits: Array[Int]
  
  def loadDirectory(fs: FileSystem, pointer: DirectoryPointer)(implicit ec: ExecutionContext): Future[Directory]
  def loadDirectory(fs: FileSystem, inode: DirectoryInode): Directory
}