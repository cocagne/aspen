package com.ibm.aspen.cumulofs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait BaseFile {
  val pointer: InodePointer
  val fs: FileSystem
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit]
  
  def mode: Int
  def uid: Int
  def gid: Int
  def ctime: Timespec
  def mtime: Timespec
  def atime: Timespec
 
  def setMode(newMode: Int)(implicit ec: ExecutionContext): Future[Unit]
  
  def setUID(uid: Int)(implicit ec: ExecutionContext): Future[Unit]
  
  def setGID(gid: Int)(implicit ec: ExecutionContext): Future[Unit]
  
  def setCtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit]
  
  def setMtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit]
  
  def setAtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit]
}