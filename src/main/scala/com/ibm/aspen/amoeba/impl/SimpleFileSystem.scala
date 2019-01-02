package com.ibm.aspen.amoeba.impl

import java.util.UUID

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.ibm.aspen.base.{AspenSystem, ObjectAllocater}
import com.ibm.aspen.base.task.LocalTaskGroup
import com.ibm.aspen.base.tieredlist.{MutableKeyValueObjectRootManager, MutableTieredKeyValueList, SimpleTieredKeyValueListNodeAllocater, TieredKeyValueListRoot}
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, KeyValueObjectState}
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.amoeba._
import com.ibm.aspen.util._

import scala.concurrent.{ExecutionContext, Future}

object SimpleFileSystem {
  def load(
      system: AspenSystem, 
      fileSystemRoot: KeyValueObjectPointer, 
      clientUUID: UUID)(implicit ec: ExecutionContext): Future[FileSystem] = {
    
    def getLocalTaskGroup(t: MutableTieredKeyValueList, allocaterUUID: UUID): Future[LocalTaskGroup] = {
      val taskGroupKey = Key(clientUUID)
      
      system.retryStrategy.retryUntilSuccessful {
        t.get(taskGroupKey).flatMap {
          case Some(v) => system.readObject(KeyValueObjectPointer(v.value)) flatMap { kvos => LocalTaskGroup.createExecutor(system, kvos) }
          
          case None => 
            
            val ffgroup = system.transact { implicit tx =>

              tx.note(s"Creating new LocalTaskGroup for SimpleFileSystem")

              val txreqs = KeyValueUpdate.KVRequirement(taskGroupKey, HLCTimestamp.now, KeyValueUpdate.TimestampRequirement.DoesNotExist) :: Nil
              
              for {
                node <- t.fetchMutableNode(taskGroupKey)
            
                (ptr, fgroup) <- LocalTaskGroup.prepareGroupAllocation(system, node.kvos.pointer, node.kvos.revision, allocaterUUID)
            
                _ <- node.prepreUpdateTransaction(List((taskGroupKey, ptr.kvPointer.toArray)), Nil, txreqs)
              } yield fgroup
            }
            
            ffgroup.flatMap( fgroup => fgroup )
        }
      }
    }
      
    for {
      rootKvos <- system.readObject(fileSystemRoot)
      tgtRoot = FileSystem.getLocalTaskGroupTreeRoot(rootKvos)
      rootMgr = new MutableKeyValueObjectRootManager(system, fileSystemRoot, FileSystem.LocalTaskGroupsTreeKey, tgtRoot)
      tgt = new MutableTieredKeyValueList(rootMgr)
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
    rootKvos: KeyValueObjectState,
    inodeCacheMax: Int = 1024,
    writeBufferSize: Int = 4 * 1024 * 1024) extends FileSystem {

  protected val fileFactory: FileFactory = new SimpleFileFactory(writeBufferSize)
  
  val fileSystemRoot: KeyValueObjectPointer = rootKvos.pointer
  
  val uuid: UUID = fileSystemRoot.uuid
  
  val directoryTableAllocaters: Array[UUID] = decodeUUIDArray(rootKvos.contents(FileSystem.DirectoryTableAllocatersArrayKey).value) 
  val directoryTableSizes: Array[Int]       = decodeIntArray(rootKvos.contents(FileSystem.DirectoryTableSizesKey).value)
  val dataTableAllocaters: Array[UUID]      = decodeUUIDArray(rootKvos.contents(FileSystem.DataTableAllocatersArrayKey).value) 
  val dataTableSizes: Array[Int]            = decodeIntArray(rootKvos.contents(FileSystem.DataTableSizesKey).value)
  val inodeAllocater: UUID                  = byte2uuid(rootKvos.contents(FileSystem.InodeAllocaterKey).value)
  val defaultSegmentSize: Int = com.ibm.aspen.amoeba.arr2int(rootKvos.contents(FileSystem.DefaultFileSegmentSizeKey).value)
  val inodeKVPairLimits: Array[Int]         = decodeIntArray(rootKvos.contents(FileSystem.KVPairLimitsKey).value)
  val directoryTableKVPairLimits: Array[Int] = decodeIntArray(rootKvos.contents(FileSystem.KVPairLimitsKey).value)
      
  private lazy val fdefaultSegmentAllocater = system.getObjectAllocater(byte2uuid(rootKvos.contents(FileSystem.DefaultFileSegmentAllocationPoolKey).value))

  private[this] val fileCache: Cache[Long, BaseFile] = Scaffeine().
    maximumSize(inodeCacheMax).
    build[Long, BaseFile]()
  
  def defaultSegmentAllocater(): Future[ObjectAllocater] = fdefaultSegmentAllocater
  
  val inodeTable: InodeTable = {
    val initialRoot = TieredKeyValueListRoot(rootKvos.contents(FileSystem.InodeTableKey).value)
    val rootMgr = new MutableKeyValueObjectRootManager(system, fileSystemRoot, FileSystem.InodeTableKey, initialRoot)
    val mtkvl = new MutableTieredKeyValueList(rootMgr)
    
    new SimpleInodeTable(system, inodeAllocater, mtkvl)
  }
  
  val inodeLoader: InodeLoader = new SimpleInodeLoader(system, inodeTable)

  protected def getCachedFile(inodeNumber: Long): Option[BaseFile] = fileCache.getIfPresent(inodeNumber)

  protected def cacheFile(file: BaseFile): Unit = fileCache.put(file.pointer.number, file)
  
  def getLocalTasksCompleted: Future[Unit] = localTaskGroup.whenAllTasksComplete()
  
  def getDataTableNodeSize(tierNumber: Int): Int = if (tierNumber < dataTableSizes.length) dataTableSizes(tierNumber) else {
    dataTableSizes(dataTableSizes.length-1)
  }
  
  def getDataTableNodeAllocater(tierNumber: Int): Future[ObjectAllocater] = {
    val allocaterUUID = if (tierNumber < dataTableAllocaters.length) dataTableAllocaters(tierNumber) else dataTableAllocaters(dataTableAllocaters.length-1)
    system.getObjectAllocater(allocaterUUID)
  }

  def directoryTableConfig: (UUID, DataBuffer) = {
    val allocaterType = SimpleTieredKeyValueListNodeAllocater.typeUUID

    val allocaterConfig = SimpleTieredKeyValueListNodeAllocater.encode(directoryTableAllocaters,
      directoryTableSizes, directoryTableKVPairLimits)

    (allocaterType, allocaterConfig)
  }
  
  FileSystem.register(this)
  
  // Ensure we resume the local task group after the file system is registered as many tasks will likely need
  // access to this object
  localTaskGroup.resume()
}