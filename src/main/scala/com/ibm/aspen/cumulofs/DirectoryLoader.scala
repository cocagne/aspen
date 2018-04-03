package com.ibm.aspen.cumulofs

import java.util.UUID

trait DirectoryLoader {
  
  def dataTableAllocaters: Array[UUID]
  def dataTableSizes: Array[Int]
  
  def loadDirectory(fs: FileSystem, pointer: DirectoryPointer): Directory
}