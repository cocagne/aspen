package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.DirectoryLoader
import com.ibm.aspen.cumulofs.InodeLoader
import com.ibm.aspen.cumulofs.Directory
import com.ibm.aspen.cumulofs.DirectoryPointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.cumulofs.FileSystem
import java.util.UUID
import com.ibm.aspen.cumulofs.DirectoryInode

class SimpleDirectoryLoader(
    val directoryTableAllocaters: Array[UUID],
    val directoryTableSizes: Array[Int]
    ) extends DirectoryLoader {
 
  override def loadDirectory(fs: FileSystem, pointer: DirectoryPointer)(implicit ec: ExecutionContext): Future[Directory] = fs.inodeLoader.load(pointer) map { inode =>
    new SimpleDirectory(inode, fs)
  }
  
  override def loadDirectory(fs: FileSystem, inode: DirectoryInode): Directory = new SimpleDirectory(inode, fs)
}