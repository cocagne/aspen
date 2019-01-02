package com.ibm.aspen.amoeba

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.amoeba.impl.CreateFileTask

trait Directory extends BaseFile {
  val pointer: DirectoryPointer
  val fs: FileSystem

  def inode: DirectoryInode

  def getInode()(implicit ec: ExecutionContext): Future[(DirectoryInode, ObjectRevision)] = {
    fs.inodeLoader.load(pointer).map(t => (t._1.asInstanceOf[DirectoryInode], t._2))
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

  def prepareHardLink(name: String, file: BaseFile)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  /** Ensures the directory is empty and that all resources are cleaned up if the transaction successfully commits 
   */
  def prepareForDirectoryDeletion()(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  def prepareCreateDirectory(name: String, mode: Int, uid: Int, gid: Int)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[DirectoryPointer]] = {
    val newInode = DirectoryInode.init(mode, uid, gid, Some(pointer))
    
    CreateFileTask.prepareFileCreation(fs, pointer, name, newInode).map(_.asInstanceOf[Future[DirectoryPointer]])
  }
  
  def prepareCreateFile(name: String, mode: Int, uid: Int, gid: Int)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[FilePointer]] = {
    val newInode = FileInode.init(mode, uid, gid)
    
    CreateFileTask.prepareFileCreation(fs, pointer, name, newInode).map(_.asInstanceOf[Future[FilePointer]])
  }

  def prepareCreateSymlink(name: String, mode: Int, uid: Int, gid: Int, link: String)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[SymlinkPointer]] = {
    val newInode = SymlinkInode.init(mode, uid, gid, link)
    
    CreateFileTask.prepareFileCreation(fs, pointer, name, newInode).map(_.asInstanceOf[Future[SymlinkPointer]])
  }
  
  def prepareCreateUnixSocket(name: String, mode: Int, uid: Int, gid: Int)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[UnixSocketPointer]] = {
    val newInode = UnixSocketInode.init(mode, uid, gid)
    
    CreateFileTask.prepareFileCreation(fs, pointer, name, newInode).map(_.asInstanceOf[Future[UnixSocketPointer]])
  }
  
  def prepareCreateFIFO(name: String, mode: Int, uid: Int, gid: Int)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[FIFOPointer]] = {
    val newInode = FIFOInode.init(mode, uid, gid)
    
    CreateFileTask.prepareFileCreation(fs, pointer, name, newInode).map(_.asInstanceOf[Future[FIFOPointer]])
  }
  
  def prepareCreateCharacterDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[CharacterDevicePointer]] = {
    val newInode = CharacterDeviceInode.init(mode, uid, gid, rdev)
    
    CreateFileTask.prepareFileCreation(fs, pointer, name, newInode).map(_.asInstanceOf[Future[CharacterDevicePointer]])
  }
  
  def prepareCreateBlockDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[BlockDevicePointer]] = {
    val newInode = BlockDeviceInode.init(mode, uid, gid, rdev)
    
    CreateFileTask.prepareFileCreation(fs, pointer, name, newInode).map(_.asInstanceOf[Future[BlockDevicePointer]])
  }
}