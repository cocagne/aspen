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
  
  val fileLoader: FileLoader
  
  val localTaskGroup: TaskGroupInterface
  
  def defaultSegmentSize: Int
  
  def defaultSegmentAllocater(): Future[ObjectAllocater]
  
  def lookup(inodeNumber: Long)(implicit ec: ExecutionContext): Future[Option[BaseFile]] = inodeTable.lookup(inodeNumber) flatMap { optr =>
    optr match {
      case None => Future.successful(None)
      case Some(iptr) => lookup(iptr).map(Some(_))
    }
  }
  
  def lookup(iptr: InodePointer)(implicit ec: ExecutionContext): Future[BaseFile] =  iptr match {
    case ptr: DirectoryPointer => loadDirectory(ptr)
    case ptr: FilePointer => loadFile(ptr)
    case ptr: SymlinkPointer => loadSymlink(ptr)
    case ptr: UnixSocketPointer => loadUnixSocket(ptr)
    case ptr: FIFOPointer => loadFIFO(ptr)
    case ptr: CharacterDevicePointer => loadCharacterDevice(ptr)
    case ptr: BlockDevicePointer => loadBlockDevice(ptr)
  }
  
  def loadRoot()(implicit ec: ExecutionContext): Future[Directory] = {
    inodeTable.lookupRoot() flatMap { pointer => directoryLoader.loadDirectory(this, pointer) }
  }
  
  def loadDirectory(pointer: DirectoryPointer)(implicit ec: ExecutionContext): Future[Directory] = directoryLoader.loadDirectory(this, pointer)
  def loadDirectory(inode: DirectoryInode): Directory = directoryLoader.loadDirectory(this, inode)
  
  def loadFile(pointer: FilePointer)(implicit ec: ExecutionContext): Future[File] = fileLoader.loadFile(this, pointer)
  
  def loadSymlink(pointer: SymlinkPointer)(implicit ec: ExecutionContext): Future[Symlink]
  
  def loadUnixSocket(pointer: UnixSocketPointer)(implicit ec: ExecutionContext): Future[UnixSocket]
  
  def loadFIFO(pointer: FIFOPointer)(implicit ec: ExecutionContext): Future[FIFO]
  
  def loadCharacterDevice(pointer: CharacterDevicePointer)(implicit ec: ExecutionContext): Future[CharacterDevice]
  
  def loadBlockDevice(pointer: BlockDevicePointer)(implicit ec: ExecutionContext): Future[BlockDevice]
  
  def getDataTableNodeSize(tierNumber: Int): Int
  
  def getDataTableNodeAllocater(tierNumber: Int): Future[ObjectAllocater]
}

object FileSystem {
  
  val InodeTableKey                       = Key(0)
  val DirectoryTableAllocatersArrayKey    = Key(1)
  val DirectoryTableSizesKey              = Key(2)
  val DataTableAllocatersArrayKey         = Key(3)
  val DataTableSizesKey                   = Key(4)
  val InodeAllocaterKey                   = Key(5)
  val LocalTaskGroupsTreeKey              = Key(6)
  val DefaultFileSegmentAllocationPoolKey = Key(7)
  val DefaultFileSegmentSizeKey           = Key(8)
  
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
      dataTableSizes: Array[Int],
      defaultAllocationPool: UUID,
      defaultFileSegmentSize: Int
      )(implicit tx: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    
    import FileMode._
    
    val rootDirMode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH  
    
    val (rootOps, rootContent) = DirectoryInode.getInitialContent(rootDirMode, 0, 0, None)
    
    
    for {
      rootDirObj <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, rootOps)
    
      rootDirPtr = new DirectoryPointer(InodeTable.RootInode, rootDirObj)
      
      inodeTblContent = KeyValueOperation.insertOperations(List((Key(InodeTable.RootInode), rootDirPtr.toArray)), tx.timestamp())
      
      rootInodeTblPtr <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, inodeTblContent)
      
      inodeTblRoot = new TieredKeyValueList.Root(0, inodeTableAllocaters, inodeTableSizes, IntegerKeyOrdering, rootInodeTblPtr)
      
      lgtgTier0 <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, Nil)
      lgtgRoot = new TieredKeyValueList.Root(0, inodeTableAllocaters, inodeTableSizes, ByteArrayKeyOrdering, lgtgTier0)
      
      icontent = List(
          (InodeTableKey,                       inodeTblRoot.toArray),
          (InodeAllocaterKey,                   uuid2byte(inodeAllocater)),
          (DirectoryTableAllocatersArrayKey,    encodeUUIDArray(directoryTableAllocaters)),
          (DirectoryTableSizesKey,              encodeIntArray(directoryTableSizes)),
          (DataTableAllocatersArrayKey,         encodeUUIDArray(dataTableAllocaters)),
          (DataTableSizesKey,                   encodeIntArray(dataTableSizes)),
          (LocalTaskGroupsTreeKey,              lgtgRoot.toArray),
          (DefaultFileSegmentAllocationPoolKey, uuid2byte(defaultAllocationPool)),
          (DefaultFileSegmentSizeKey,           int2arr(defaultFileSegmentSize)))
      
      fsObjContent = KeyValueOperation.insertOperations(icontent, tx.timestamp())
      
      fsObjPtr <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, fsObjContent)
      
    } yield fsObjPtr
  }
}