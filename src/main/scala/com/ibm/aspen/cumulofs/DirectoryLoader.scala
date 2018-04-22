package com.ibm.aspen.cumulofs

import java.util.UUID

trait DirectoryLoader {
  
  def directoryTableAllocaters: Array[UUID]
  def directoryTableSizes: Array[Int]
  
  def loadDirectory(fs: FileSystem, pointer: DirectoryPointer): Directory
}