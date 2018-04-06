package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.tieredlist.SimpleMutableTieredKeyValueList
import com.ibm.aspen.core.objects.keyvalue.IntegerKeyOrdering
import com.ibm.aspen.cumulofs.InodeTable
import com.ibm.aspen.cumulofs.InodeLoader
import com.ibm.aspen.cumulofs.DirectoryLoader
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.cumulofs.{decodeIntArray, decodeUUIDArray}
import java.util.UUID
import com.ibm.aspen.base.task.LocalTaskGroup
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.util._
import scala.concurrent.Future

class SimpleFileSystem(
    val system: AspenSystem,
    val localTaskGroup: LocalTaskGroup,
    rootKvos: KeyValueObjectState) extends FileSystem {
  
  val fileSystemRoot = rootKvos.pointer
  
  val uuid: UUID = fileSystemRoot.uuid
  
  val directoryTableAllocaters: Array[UUID] = decodeUUIDArray(rootKvos.contents(FileSystem.DirectoryTableAllocatersArrayKey).value) 
  val directoryTableSizes: Array[Int]       = decodeIntArray(rootKvos.contents(FileSystem.DirectoryTableSizesKey).value)
  val dataTableAllocaters: Array[UUID]      = decodeUUIDArray(rootKvos.contents(FileSystem.DataTableAllocatersArrayKey).value) 
  val dataTableSizes: Array[Int]            = decodeIntArray(rootKvos.contents(FileSystem.DataTableSizesKey).value)
  val inodeAllocater: UUID                  = byte2uuid(rootKvos.contents(FileSystem.InodeAllocaterKey).value)
  
  val inodeTable: InodeTable = new SimpleInodeTable(
      system, 
      inodeAllocater, 
      new SimpleMutableTieredKeyValueList(system, Left(fileSystemRoot), FileSystem.InodeTableKey, IntegerKeyOrdering))
  
  val inodeLoader: InodeLoader = new SimpleInodeLoader(system, inodeTable, new NoInodeCache)
  
  val directoryLoader: DirectoryLoader = new SimpleDirectoryLoader(directoryTableAllocaters, directoryTableSizes)
  
  FileSystem.register(this)
}