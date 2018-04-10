package com.ibm.aspen.cumulofs

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.ibm.aspen.base.Transaction

trait Directory {
  val pointer: DirectoryPointer
  val fs: FileSystem
  
  def getInode()(implicit ec: ExecutionContext): Future[DirectoryInode] = {
    fs.inodeLoader.loadDirectory(pointer)
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
  
  /** Ensures the directory is empty and that all resources are cleaned up if the transaction successfully commits 
   */
  def prepareForDirectoryDeletion()(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  
  def createDirectory(name: String, mode: Int, uid: Int, gid: Int)(implicit ec: ExecutionContext): Future[DirectoryPointer] = {
    val (initialOps, initalContent) = DirectoryInode.getInitialContent(mode, uid, gid, Some(pointer))
    
    CreateFileTask.execute(fs, pointer, name, FileType.Directory, initialOps).map(_.asInstanceOf[DirectoryPointer])
  }

}