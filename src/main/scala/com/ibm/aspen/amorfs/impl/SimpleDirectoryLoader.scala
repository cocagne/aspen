package com.ibm.aspen.amorfs.impl

import java.util.UUID

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.amorfs._

import scala.concurrent.{ExecutionContext, Future}

class SimpleDirectoryLoader(
    val directoryTableAllocaters: Array[UUID],
    val directoryTableSizes: Array[Int],
    val directoryTableKVPairLimits: Array[Int]
    ) extends DirectoryLoader {
 
  override def loadDirectory(fs: FileSystem,
                             pointer: DirectoryPointer)(implicit ec: ExecutionContext): Future[Directory] = {
    fs.inodeLoader.load(pointer) map { t =>
      val (inode, revision) = t
      new SimpleDirectory(pointer, revision, inode, fs)
    }
  }
  
  override def loadDirectory(fs: FileSystem,
                             pointer: DirectoryPointer,
                             revision: ObjectRevision,
                             inode: DirectoryInode): Directory = {
    new SimpleDirectory(pointer, revision, inode, fs)
  }
}