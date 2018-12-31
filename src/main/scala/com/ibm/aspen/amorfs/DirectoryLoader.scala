package com.ibm.aspen.amorfs

import java.util.UUID

import com.ibm.aspen.core.objects.ObjectRevision

import scala.concurrent.{ExecutionContext, Future}

trait DirectoryLoader {
  
  def directoryTableAllocaters: Array[UUID]
  def directoryTableSizes: Array[Int]
  def directoryTableKVPairLimits: Array[Int]
  
  def loadDirectory(fs: FileSystem, pointer: DirectoryPointer)(implicit ec: ExecutionContext): Future[Directory]
  def loadDirectory(fs: FileSystem,
                    pointer: DirectoryPointer,
                    revision: ObjectRevision,
                    inode: DirectoryInode): Directory
}