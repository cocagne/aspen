package com.ibm.aspen.cumulofs

import java.util.UUID

import com.ibm.aspen.base.{AspenSystem, ObjectAllocater, Transaction}
import com.ibm.aspen.base.task.TaskGroupInterface
import com.ibm.aspen.base.tieredlist.{SimpleTieredKeyValueListNodeAllocater, TieredKeyValueListRoot}
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, KeyValueObjectState, ObjectPointer, ObjectRevision}
import com.ibm.aspen.core.objects.keyvalue.{ByteArrayKeyOrdering, Insert, IntegerKeyOrdering, Key}
import com.ibm.aspen.util._

import scala.concurrent.{ExecutionContext, Future}

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

  def lookupInodePointer(inodeNumber: Long)(implicit ec: ExecutionContext): Future[Option[InodePointer]] = {
    inodeTable.lookup(inodeNumber)
  }
  
  def lookup(inodeNumber: Long)(implicit ec: ExecutionContext): Future[Option[BaseFile]] = inodeTable.lookup(inodeNumber) flatMap {
    case None => Future.successful(None)
    case Some(iptr) => lookup(iptr).map(Some(_))
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
  def loadDirectory(pointer: DirectoryPointer, inode: DirectoryInode, revision: ObjectRevision): Directory = {
    directoryLoader.loadDirectory(this, pointer, revision, inode)
  }
  
  def loadFile(pointer: FilePointer)(implicit ec: ExecutionContext): Future[File] = fileLoader.loadFile(this, pointer)
  
  def loadSymlink(pointer: SymlinkPointer)(implicit ec: ExecutionContext): Future[Symlink]
  
  def loadUnixSocket(pointer: UnixSocketPointer)(implicit ec: ExecutionContext): Future[UnixSocket]
  
  def loadFIFO(pointer: FIFOPointer)(implicit ec: ExecutionContext): Future[FIFO]
  
  def loadCharacterDevice(pointer: CharacterDevicePointer)(implicit ec: ExecutionContext): Future[CharacterDevice]
  
  def loadBlockDevice(pointer: BlockDevicePointer)(implicit ec: ExecutionContext): Future[BlockDevice]
  
  def getDataTableNodeSize(tierNumber: Int): Int
  
  def getDataTableNodeAllocater(tierNumber: Int): Future[ObjectAllocater]

  def directoryTableConfig: (UUID, DataBuffer) // Alloc config for new TieredKeyValueListRoot instances
  
  /** Returns a future to the completion of all currently active local tasks
   *  
   *  This is primarily intended for use in unit tests. 
   */
  def getLocalTasksCompleted: Future[Unit]
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
  val KVPairLimitsKey                     = Key(9)
  
  private[this] var loadedFileSystems = Map[UUID, FileSystem]()
  
  def register(fs: FileSystem): Unit = synchronized {
    loadedFileSystems += (fs.uuid -> fs)
  }
  
  def getRegisteredFileSystem(uuid: UUID): Option[FileSystem] = synchronized {
    loadedFileSystems.get(uuid)
  }
  
  def getLocalTaskGroupTreeRoot(rootKvos: KeyValueObjectState): TieredKeyValueListRoot = {
    TieredKeyValueListRoot(rootKvos.contents(LocalTaskGroupsTreeKey).value)
  }
  
  def getInodeAllocater(rootKvos: KeyValueObjectState): UUID = {
    byte2uuid(rootKvos.contents(InodeAllocaterKey).value)
  }
  
  /** Creates a new CumuloFS file system as part of the supplied Transaction.
   *  
   *  @return Pointer to the file system root object
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
      inodeKVPairLimits: Array[Int],
      directoryTableAllocaters: Array[UUID], // For Directory entry Tiered List
      directoryTableSizes: Array[Int],
      dataTableAllocaters: Array[UUID],      // For File Data Tiered List
      dataTableSizes: Array[Int],
      fileSegmentAllocationPool: UUID,
      fileSegmentSize: Int
      )(implicit tx: Transaction, ec: ExecutionContext): Future[KeyValueObjectPointer] = {
    
    import FileMode._
    
    val rootDirMode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH

    val rootInode = DirectoryInode.init(rootDirMode, 0, 0, None)
    
    val allocaterType = SimpleTieredKeyValueListNodeAllocater.typeUUID
    val allocaterConfig = SimpleTieredKeyValueListNodeAllocater.encode(inodeTableAllocaters, inodeTableSizes, inodeKVPairLimits)
    
    for {
      rootDirObj <- allocater.allocateDataObject(allocatingObject, allocatingObjectRevision, rootInode.toDataBuffer)
    
      rootDirPtr = new DirectoryPointer(InodeTable.RootInode, rootDirObj)
      
      inodeTblContent = Insert(Key(InodeTable.RootInode), rootDirPtr.toArray) :: Nil
      
      rootInodeTblPtr <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, inodeTblContent)
      
      inodeTblRoot = TieredKeyValueListRoot(0, IntegerKeyOrdering, rootInodeTblPtr, allocaterType, allocaterConfig)
      
      lgtgTier0 <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, Nil)
      
      lgtgRoot = TieredKeyValueListRoot(0, ByteArrayKeyOrdering, lgtgTier0, allocaterType, allocaterConfig)
      
      icontent = List(
          (InodeTableKey,                       inodeTblRoot.toArray),
          (InodeAllocaterKey,                   uuid2byte(inodeAllocater)),
          (DirectoryTableAllocatersArrayKey,    encodeUUIDArray(directoryTableAllocaters)),
          (DirectoryTableSizesKey,              encodeIntArray(directoryTableSizes)),
          (DataTableAllocatersArrayKey,         encodeUUIDArray(dataTableAllocaters)),
          (DataTableSizesKey,                   encodeIntArray(dataTableSizes)),
          (LocalTaskGroupsTreeKey,              lgtgRoot.toArray),
          (DefaultFileSegmentAllocationPoolKey, uuid2byte(fileSegmentAllocationPool)),
          (DefaultFileSegmentSizeKey,           int2arr(fileSegmentSize)),
          (KVPairLimitsKey,                     encodeIntArray(inodeKVPairLimits)))
      
      fsObjContent = icontent.map(t => Insert(t._1, t._2))
      
      fsObjPtr <- allocater.allocateKeyValueObject(allocatingObject, allocatingObjectRevision, fsObjContent)
      
    } yield fsObjPtr
  }
}