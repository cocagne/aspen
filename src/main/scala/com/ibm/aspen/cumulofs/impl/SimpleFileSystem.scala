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
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.cumulofs.FileLoader
import com.ibm.aspen.cumulofs.Symlink
import com.ibm.aspen.cumulofs.SymlinkPointer
import com.ibm.aspen.cumulofs.UnixSocketPointer
import com.ibm.aspen.cumulofs.UnixSocket
import com.ibm.aspen.cumulofs.FIFOPointer
import com.ibm.aspen.cumulofs.FIFO
import com.ibm.aspen.cumulofs.CharacterDevicePointer
import com.ibm.aspen.cumulofs.BlockDevice
import com.ibm.aspen.cumulofs.CharacterDevice
import com.ibm.aspen.cumulofs.BlockDevicePointer

object SimpleFileSystem {
  def load(
      system: AspenSystem, 
      fileSystemRoot: KeyValueObjectPointer, 
      clientUUID: UUID)(implicit ec: ExecutionContext): Future[FileSystem] = {
    
    def getLocalTaskGroup(t: SimpleMutableTieredKeyValueList, allocaterUUID: UUID): Future[LocalTaskGroup] = {
      val taskGroupKey = Key(clientUUID)
      
      system.retryStrategy.retryUntilSuccessful {
        t.get(taskGroupKey).flatMap { o => o match {
          case Some(v) => system.readObject(KeyValueObjectPointer(v.value)) flatMap { kvos => LocalTaskGroup.createExecutor(system, kvos) }
          
          case None => 
            
            val ffgroup = system.transact { implicit tx =>
            
              val txreqs = KeyValueUpdate.KVRequirement(taskGroupKey, tx.timestamp(), KeyValueUpdate.TimestampRequirement.DoesNotExist) :: Nil
              
              for {
                node <- t.fetchMutableNode(taskGroupKey)
            
                (ptr, fgroup) <- LocalTaskGroup.prepareGroupAllocation(system, node.kvos.pointer, node.kvos.revision, allocaterUUID)
            
                ready <- node.prepreUpdateTransaction(List((taskGroupKey, ptr.kvPointer.toArray)), Nil, txreqs)
              } yield fgroup
            }
            
            ffgroup.flatMap( fgroup => fgroup )
        }}
      }
    }
      
    for {
      rootKvos <- system.readObject(fileSystemRoot)
      tgtRoot = FileSystem.getLocalTaskGroupTree(rootKvos)
      tgt = new SimpleMutableTieredKeyValueList(system, Left(fileSystemRoot), FileSystem.LocalTaskGroupsTreeKey, ByteArrayKeyOrdering, Some(tgtRoot))
      allocaterUUID = FileSystem.getInodeAllocater(rootKvos)
      taskGroup <- getLocalTaskGroup(tgt, allocaterUUID)
    } yield {
      new SimpleFileSystem(system, taskGroup, rootKvos)
    }
  } 
}

class SimpleFileSystem private (
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
  val defaultSegmentSize: Int = com.ibm.aspen.cumulofs.arr2int(rootKvos.contents(FileSystem.DefaultFileSegmentSizeKey).value)
      
  private lazy val fdefaultSegmentAllocater = system.getObjectAllocater(byte2uuid(rootKvos.contents(FileSystem.DefaultFileSegmentAllocationPoolKey).value))
  
  def defaultSegmentAllocater(): Future[ObjectAllocater] = fdefaultSegmentAllocater
  
  val inodeTable: InodeTable = new SimpleInodeTable(
      system, 
      inodeAllocater, 
      new SimpleMutableTieredKeyValueList(system, Left(fileSystemRoot), FileSystem.InodeTableKey, IntegerKeyOrdering))
  
  val inodeLoader: InodeLoader = new SimpleInodeLoader(system, inodeTable, new NoInodeCache)
  
  val directoryLoader: DirectoryLoader = new SimpleDirectoryLoader(directoryTableAllocaters, directoryTableSizes)
  
  val fileLoader: FileLoader = new SimpleFileLoader
  
  def getLocalTasksCompleted(): Future[Unit] = localTaskGroup.getAllTasksComplete()
  
  def getDataTableNodeSize(tierNumber: Int): Int = if (tierNumber < dataTableSizes.length) dataTableSizes(tierNumber) else {
    dataTableSizes(dataTableSizes.length-1)
  }
  
  def getDataTableNodeAllocater(tierNumber: Int): Future[ObjectAllocater] = {
    val allocaterUUID = if (tierNumber < dataTableAllocaters.length) dataTableAllocaters(tierNumber) else dataTableAllocaters(dataTableAllocaters.length-1)
    system.getObjectAllocater(allocaterUUID)
  }
  
  def loadSymlink(pointer: SymlinkPointer)(implicit ec: ExecutionContext): Future[Symlink] = inodeLoader.load(pointer).map(new SimpleSymlink(_,this)) 
  
  def loadUnixSocket(pointer: UnixSocketPointer)(implicit ec: ExecutionContext): Future[UnixSocket] = inodeLoader.load(pointer).map(new SimpleUnixSocket(_,this))
  
  def loadFIFO(pointer: FIFOPointer)(implicit ec: ExecutionContext): Future[FIFO] = inodeLoader.load(pointer).map(new SimpleFIFO(_,this))
  
  def loadCharacterDevice(pointer: CharacterDevicePointer)(implicit ec: ExecutionContext): Future[CharacterDevice] = inodeLoader.load(pointer).map(new SimpleCharacterDevice(_,this))
  
  def loadBlockDevice(pointer: BlockDevicePointer)(implicit ec: ExecutionContext): Future[BlockDevice] = inodeLoader.load(pointer).map(new SimpleBlockDevice(_,this))
  
  FileSystem.register(this)
  
  // Ensure we resume the local task group after the file system is registered as many tasks will likely need
  // access to this object
  localTaskGroup.resume()
}