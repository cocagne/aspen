package com.ibm.aspen.cumulofs

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

trait Directory {
  val pointer: DirectoryPointer
  val fs: FileSystem
  
  def getInode()(implicit ec: ExecutionContext): Future[DirectoryInode] = {
    fs.inodeLoader.loadDirectory(pointer)
  }
  
  def getContents()(implicit ec: ExecutionContext): Future[List[DirectoryEntry]]
  
  def lookup(name: String)(implicit ec: ExecutionContext): Future[Option[DirectoryEntry]]
  
  def insert(name: String, pointer: InodePointer)(implicit ec: ExecutionContext): Future[Unit]
  
  def delete(name: String)(implicit ec: ExecutionContext): Future[Unit]
}