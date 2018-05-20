package com.ibm.aspen.cumulofs

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Transaction

trait Directory extends BaseFile {
  val pointer: DirectoryPointer
  val fs: FileSystem
  
  def getInode()(implicit ec: ExecutionContext): Future[DirectoryInode] = {
    fs.inodeLoader.load(pointer)
  }
  
  def lookup(name: String)(implicit ec: ExecutionContext): Future[Option[InodePointer]] = name match {
    case "." => Future.successful(Some(pointer))
    case ".." => getInode().map( _.parentDirectoryPointer )
    case _ => getEntry(name)
  }
  
  def getContents()(implicit ec: ExecutionContext): Future[List[DirectoryEntry]]
  
  def getEntry(name: String)(implicit ec: ExecutionContext): Future[Option[InodePointer]]
  
  def prepareInsert(name: String, pointer: InodePointer)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  def prepareDelete(name: String)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  def delete(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    fs.system.transact { implicit tx => prepareDelete(name) }
  }
  
  def hardLink(name: String, file: BaseFile)(implicit ec: ExecutionContext): Future[Unit]
  
  /** Ensures the directory is empty and that all resources are cleaned up if the transaction successfully commits 
   */
  def prepareForDirectoryDeletion()(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  def createDirectory(name: String, mode: Int, uid: Int, gid: Int)(implicit ec: ExecutionContext): Future[DirectoryPointer] = {
    val (initialOps, initalContent) = DirectoryInode.getInitialContent(mode, uid, gid, Some(pointer))
    
    CreateFileTask.execute(fs, pointer, name, FileType.Directory, initialOps).map(_.asInstanceOf[DirectoryPointer])
  }
  
  def createFile(name: String, mode: Int, uid: Int, gid: Int)(implicit ec: ExecutionContext): Future[FilePointer] = {
    val (initialOps, initalContent) = FileInode.getInitialContent(mode, uid, gid)
    
    CreateFileTask.execute(fs, pointer, name, FileType.File, initialOps).map(_.asInstanceOf[FilePointer])
  }

  def createSymlink(name: String, mode: Int, uid: Int, gid: Int, link: String)(implicit ec: ExecutionContext): Future[SymlinkPointer] = {
    val (initialOps, initalContent) = SymlinkInode.getInitialContent(mode, uid, gid, link)
    
    CreateFileTask.execute(fs, pointer, name, FileType.Symlink, initialOps).map(_.asInstanceOf[SymlinkPointer])
  }
  
  def createUnixSocket(name: String, mode: Int, uid: Int, gid: Int)(implicit ec: ExecutionContext): Future[UnixSocketPointer] = {
    val (initialOps, initalContent) = UnixSocketInode.getInitialContent(mode, uid, gid)
    
    CreateFileTask.execute(fs, pointer, name, FileType.UnixSocket, initialOps).map(_.asInstanceOf[UnixSocketPointer])
  }
  
  def createFIFO(name: String, mode: Int, uid: Int, gid: Int)(implicit ec: ExecutionContext): Future[FIFOPointer] = {
    val (initialOps, initalContent) = FIFOInode.getInitialContent(mode, uid, gid)
    
    CreateFileTask.execute(fs, pointer, name, FileType.FIFO, initialOps).map(_.asInstanceOf[FIFOPointer])
  }
  
  def createCharacterDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int)(implicit ec: ExecutionContext): Future[CharacterDevicePointer] = {
    val (initialOps, initalContent) = CharacterDeviceInode.getInitialContent(mode, uid, gid, rdev)
    
    CreateFileTask.execute(fs, pointer, name, FileType.CharacterDevice, initialOps).map(_.asInstanceOf[CharacterDevicePointer])
  }
  
  def createBlockDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int)(implicit ec: ExecutionContext): Future[BlockDevicePointer] = {
    val (initialOps, initalContent) = BlockDeviceInode.getInitialContent(mode, uid, gid, rdev)
    
    CreateFileTask.execute(fs, pointer, name, FileType.BlockDevice, initialOps).map(_.asInstanceOf[BlockDevicePointer])
  }
}