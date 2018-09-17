package com.ibm.aspen.cumulofs

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.cumulofs.impl.CreateFileTask

trait Directory extends BaseFile {
  val pointer: DirectoryPointer
  val fs: FileSystem
  
  def getInode()(implicit ec: ExecutionContext): Future[(DirectoryInode, ObjectRevision)] = {
    fs.inodeLoader.load(pointer)
  }
  
  def lookup(name: String)(implicit ec: ExecutionContext): Future[Option[InodePointer]] = name match {
    case "." => Future.successful(Some(pointer))
    case ".." => getInode().map( _._1.oparent )
    case _ => getEntry(name)
  }
  
  def getContents()(implicit ec: ExecutionContext): Future[List[DirectoryEntry]]
  
  def getEntry(name: String)(implicit ec: ExecutionContext): Future[Option[InodePointer]]
  
  def prepareInsert(name: String, pointer: InodePointer, incref: Boolean=true)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  def prepareDelete(name: String, decref: Boolean=true)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[Unit]]
  
  def prepareRename(oldName: String, newName: String)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  def delete(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    implicit val tx: Transaction = fs.system.newTransaction()

    val f = for {
      fcomplete <- prepareDelete(name)

      _ <- tx.commit()

      _ <- fcomplete
    } yield ()
    
    f.failed.foreach( tx.invalidateTransaction )
    f
  }
  
  def hardLink(name: String, file: BaseFile)(implicit ec: ExecutionContext): Future[Unit]
  
  /** Ensures the directory is empty and that all resources are cleaned up if the transaction successfully commits 
   */
  def prepareForDirectoryDeletion()(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  def createDirectory(name: String, mode: Int, uid: Int, gid: Int)(implicit ec: ExecutionContext): Future[DirectoryPointer] = {
    val newInode = DirectoryInode.init(mode, uid, gid, Some(pointer))
    
    CreateFileTask.execute(fs, pointer, name, newInode).map(_.asInstanceOf[DirectoryPointer])
  }
  
  def createFile(name: String, mode: Int, uid: Int, gid: Int)(implicit ec: ExecutionContext): Future[FilePointer] = {
    val newInode = FileInode.init(mode, uid, gid)
    
    CreateFileTask.execute(fs, pointer, name, newInode).map(_.asInstanceOf[FilePointer])
  }

  def createSymlink(name: String, mode: Int, uid: Int, gid: Int, link: String)(implicit ec: ExecutionContext): Future[SymlinkPointer] = {
    val newInode = SymlinkInode.init(mode, uid, gid, link)
    
    CreateFileTask.execute(fs, pointer, name, newInode).map(_.asInstanceOf[SymlinkPointer])
  }
  
  def createUnixSocket(name: String, mode: Int, uid: Int, gid: Int)(implicit ec: ExecutionContext): Future[UnixSocketPointer] = {
    val newInode = UnixSocketInode.init(mode, uid, gid)
    
    CreateFileTask.execute(fs, pointer, name, newInode).map(_.asInstanceOf[UnixSocketPointer])
  }
  
  def createFIFO(name: String, mode: Int, uid: Int, gid: Int)(implicit ec: ExecutionContext): Future[FIFOPointer] = {
    val newInode = FIFOInode.init(mode, uid, gid)
    
    CreateFileTask.execute(fs, pointer, name, newInode).map(_.asInstanceOf[FIFOPointer])
  }
  
  def createCharacterDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int)(implicit ec: ExecutionContext): Future[CharacterDevicePointer] = {
    val newInode = CharacterDeviceInode.init(mode, uid, gid, rdev)
    
    CreateFileTask.execute(fs, pointer, name, newInode).map(_.asInstanceOf[CharacterDevicePointer])
  }
  
  def createBlockDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int)(implicit ec: ExecutionContext): Future[BlockDevicePointer] = {
    val newInode = BlockDeviceInode.init(mode, uid, gid, rdev)
    
    CreateFileTask.execute(fs, pointer, name, newInode).map(_.asInstanceOf[BlockDevicePointer])
  }
}