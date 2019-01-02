package com.ibm.aspen.amoeba

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

  protected val fileFactory: FileFactory

  protected def getCachedFile(inodeNumber: Long): Option[BaseFile]
  protected def cacheFile(file: BaseFile): Unit

  private[this] var loading: Map[Long, Future[BaseFile]] = Map()

  // tuple is (File, OpenHandleCount)
  private[this] var openFiles: Map[Long, File] = Map()

  val localTaskGroup: TaskGroupInterface
  
  def defaultSegmentSize: Int
  
  def defaultSegmentAllocater(): Future[ObjectAllocater]

  def lookupInodePointer(inodeNumber: Long)(implicit ec: ExecutionContext): Future[Option[InodePointer]] = {
    getCachedFile(inodeNumber) match {
      case Some(f) => Future.successful(Some(f.pointer))
      case None => inodeTable.lookup(inodeNumber)
    }
  }
  
  def lookup(inodeNumber: Long)(implicit ec: ExecutionContext): Future[Option[BaseFile]] = {
    getCachedFile(inodeNumber) match {
      case Some(f) => Future.successful(Some(f))
      case None =>
        inodeTable.lookup(inodeNumber) flatMap {
          case None => Future.successful(None)
          case Some(iptr) => lookup(iptr).map(Some(_))
        }
    }
  }
  
  def lookup(iptr: InodePointer)(implicit ec: ExecutionContext): Future[BaseFile] =  {
    getCachedFile(iptr.number) match {
      case Some(f) => Future.successful(f)
      case None =>
        iptr match {
          case ptr: DirectoryPointer => loadDirectory(ptr)
          case ptr: FilePointer =>
            synchronized { openFiles.get(iptr.number) } match {
              case Some(file) => Future.successful(file)
              case None => loadFile(ptr)
            }
          case ptr: SymlinkPointer => loadSymlink(ptr)
          case ptr: UnixSocketPointer => loadUnixSocket(ptr)
          case ptr: FIFOPointer => loadFIFO(ptr)
          case ptr: CharacterDevicePointer => loadCharacterDevice(ptr)
          case ptr: BlockDevicePointer => loadBlockDevice(ptr)
        }
    }
  }

  // Called when file handles are opened so we can maintain the openFiles map
  private[amoeba] def openFileHandle(file: File): FileHandle = {
    synchronized {
      if (!file.hasOpenHandles)
        openFiles += file.pointer.number -> file
    }

    fileFactory.createFileHandle(this, file)
  }

  // Called when file handles are closed for maintaining hte openFiles map
  private[amoeba] def closeFileHandle(file: File): Unit = synchronized {
    if (!file.hasOpenHandles)
      openFiles -= file.pointer.number
  }
  
  def loadRoot()(implicit ec: ExecutionContext): Future[Directory] = {
    inodeTable.lookupRoot() flatMap { pointer => loadDirectory(pointer) }
  }

  private def doLoad[
  PointerType <: InodePointer,
  InodeType <: Inode,
  FileType <: BaseFile](pointer: PointerType,
                        createFn: (FileSystem, PointerType, InodeType, ObjectRevision) => Future[FileType])(implicit ec: ExecutionContext): Future[FileType] = {
    getCachedFile(pointer.number) match {
      case Some(f) => Future.successful(f.asInstanceOf[FileType])
      case None => synchronized {
        loading.get(pointer.number) match {
          case Some(f) => f.map(base => base.asInstanceOf[FileType])
          case None =>
            val f = inodeLoader.load(pointer).flatMap { t =>
              val (inode, revision) = t
              createFn(this, pointer, inode.asInstanceOf[InodeType], revision)
            }
            loading += (pointer.number -> f)
            f.foreach { _ =>
              synchronized {
                loading -= pointer.number
              }
            }
            f
        }
      }
    }
  }

  def loadDirectory(pointer: DirectoryPointer)(implicit ec: ExecutionContext): Future[Directory] = {
    doLoad[DirectoryPointer, DirectoryInode, Directory](pointer, fileFactory.createDirectory)
  }
  
  def loadFile(pointer: FilePointer)(implicit ec: ExecutionContext): Future[File] = {
    doLoad[FilePointer, FileInode, File](pointer, fileFactory.createFile)
  }
  
  def loadSymlink(pointer: SymlinkPointer)(implicit ec: ExecutionContext): Future[Symlink] = {
    doLoad[SymlinkPointer, SymlinkInode, Symlink](pointer, fileFactory.createSymlink)
  }
  
  def loadUnixSocket(pointer: UnixSocketPointer)(implicit ec: ExecutionContext): Future[UnixSocket] = {
    doLoad[UnixSocketPointer, UnixSocketInode, UnixSocket](pointer, fileFactory.createUnixSocket)
  }
  
  def loadFIFO(pointer: FIFOPointer)(implicit ec: ExecutionContext): Future[FIFO] = {
    doLoad[FIFOPointer, FIFOInode, FIFO](pointer, fileFactory.createFIFO)
  }
  
  def loadCharacterDevice(pointer: CharacterDevicePointer)(implicit ec: ExecutionContext): Future[CharacterDevice] = {
    doLoad[CharacterDevicePointer, CharacterDeviceInode, CharacterDevice](pointer, fileFactory.createCharacterDevice)
  }
  
  def loadBlockDevice(pointer: BlockDevicePointer)(implicit ec: ExecutionContext): Future[BlockDevice] = {
    doLoad[BlockDevicePointer, BlockDeviceInode, BlockDevice](pointer, fileFactory.createBlockDevice)
  }
  
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
  
  /** Creates a new Ameoba file system as part of the supplied Transaction.
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