package com.ibm.aspen.cumulofs

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

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
  
  def insert(name: String, pointer: InodePointer)(implicit ec: ExecutionContext): Future[Unit]
  
  def delete(name: String)(implicit ec: ExecutionContext): Future[Unit]
}