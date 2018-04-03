package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.DirectoryLoader
import com.ibm.aspen.cumulofs.InodeLoader
import com.ibm.aspen.cumulofs.Directory
import com.ibm.aspen.cumulofs.DirectoryPointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.cumulofs.FileSystem
import java.util.UUID

class SimpleDirectoryLoader(
    val dataTableAllocaters: Array[UUID],
    val dataTableSizes: Array[Int]
    ) extends DirectoryLoader {
 
  override def loadDirectory(fs: FileSystem, pointer: DirectoryPointer): Directory = {
    return new SimpleDirectory(pointer, fs)
  }
}