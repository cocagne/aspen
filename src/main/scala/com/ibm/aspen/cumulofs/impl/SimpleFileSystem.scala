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
  
  val inodeTable: InodeTable = new SimpleInodeTable(
      system, 
      inodeAllocater, 
      new SimpleMutableTieredKeyValueList(system, Left(fileSystemRoot), FileSystem.InodeTableKey, IntegerKeyOrdering))
  
  val inodeLoader: InodeLoader = new SimpleInodeLoader(system, inodeTable, new NoInodeCache)
  
  val directoryLoader: DirectoryLoader = new SimpleDirectoryLoader(directoryTableAllocaters, directoryTableSizes)
  
  FileSystem.register(this)
  
  // Ensure we resume the local task group after the file system is registered as many tasks will likely need
  // access to this object
  localTaskGroup.resume()
}