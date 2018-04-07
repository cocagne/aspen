package com.ibm.aspen.cumulofs

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.base.Transaction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.Key
import java.util.UUID
import com.ibm.aspen.core.objects.keyvalue.IntegerKeyOrdering
import com.ibm.aspen.base.tieredlist.TieredKeyValueList
import com.ibm.aspen.base.task.LocalTaskGroup
import com.ibm.aspen.util._
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.base.task.TaskGroupInterface

trait FileSystem {
  /** UUID of the FileSystem's root KeyValue object */
  val uuid: UUID 
  
  val system: AspenSystem
  
  val inodeTable: InodeTable
  
  val inodeLoader: InodeLoader
  
  val directoryLoader: DirectoryLoader
  
  val localTaskGroup: TaskGroupInterface
  
  def loadDirectory(pointer: DirectoryPointer): Directory = directoryLoader.loadDirectory(this, pointer)
}

object FileSystem {
  
  val InodeTableKey                    = Key(0)
  val DirectoryTableAllocatersArrayKey = Key(1)
  val DirectoryTableSizesKey           = Key(2)
  val DataTableAllocatersArrayKey      = Key(3)
  val DataTableSizesKey                = Key(4)
  val InodeAllocaterKey                = Key(5)
  val LocalTaskGroupsTreeKey           = Key(6)
  
  private[this] var loadedFileSystems = Map[UUID, FileSystem]()
  
  def register(fs: FileSystem): Unit = synchronized {
    loadedFileSystems += (fs.uuid -> fs)
  }
  
  def getRegisteredFileSystem(uuid: UUID): Option[FileSystem] = synchronized {
    loadedFileSystems.get(uuid)
  }
  
  def getLocalTaskGroupTree(rootKvos: KeyValueObjectState): TieredKeyValueList.Root = {
    TieredKeyValueList.Root(rootKvos.contents(LocalTaskGroupsTreeKey).value)
  }
  
  def getInodeAllocater(rootKvos: KeyValueObjectState): UUID = {
    byte2uuid(rootKvos.contents(LocalTaskGroupsTreeKey).value)
  }
  
  /** Creates a new CumuloFS file system as part of the supplied Transaction.
   *  
   *  @returns Pointer to the file system root object
   *  
   *  Involves 3 simultaneous allocations
   *    - FS root object
   *    - First leaf of the Inode Table
   *    - Root directory Inode 
   */
  def prepareNewFileSystem(
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      allocater: ObjectAllocater,
      inodeAllocater: UUID,
      inodeTableAllocaters: Array[UUID],     // For InodeTable Tiered List
      inodeTableSizes: Array[Int],
      directoryTableAllocaters: Array[UUID], // For Directory entry Tiered List
      directoryTableSizes: Array[Int],
      dataTableAllocaters: Array[UUID],      // For File Data Tiered List
      dataTableSizes: Array[Int]
      )(implicit tx: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    
    import FileMode._
    
    val rootDirMode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH  
    
    val (rootOps, rootContent) = DirectoryInode.getInitialContent(rootDirMode, 0, 0, None)
    
    for {
      rootDirObj <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, rootOps)
      
      rootDirPtr = new DirectoryPointer(0, rootDirObj)
      
      inodeTblContent = KeyValueOperation.insertOperations(List((Key(0), rootDirPtr.toArray)), tx.timestamp())
      
      rootInodeTblPtr <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, inodeTblContent)
      
      inodeTblRoot = new TieredKeyValueList.Root(0, inodeTableAllocaters, inodeTableSizes, IntegerKeyOrdering, rootInodeTblPtr)
      
      lgtgTier0 <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, Nil)
      lgtgRoot = new TieredKeyValueList.Root(0, inodeTableAllocaters, inodeTableSizes, ByteArrayKeyOrdering, lgtgTier0)
      
      icontent = List(
          (InodeTableKey,                    inodeTblRoot.toArray),
          (InodeAllocaterKey,                uuid2byte(inodeAllocater)),
          (DirectoryTableAllocatersArrayKey, encodeUUIDArray(directoryTableAllocaters)),
          (DirectoryTableSizesKey,           encodeIntArray(directoryTableSizes)),
          (DataTableAllocatersArrayKey,      encodeUUIDArray(dataTableAllocaters)),
          (DataTableSizesKey,                encodeIntArray(dataTableSizes)),
          (LocalTaskGroupsTreeKey,           lgtgRoot.toArray))
      
      fsObjContent = KeyValueOperation.insertOperations(icontent, tx.timestamp())
      
      fsObjPtr <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, fsObjContent)
      
    } yield fsObjPtr
  }
}