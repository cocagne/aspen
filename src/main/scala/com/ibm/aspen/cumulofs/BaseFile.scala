package com.ibm.aspen.cumulofs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait BaseFile {
  val pointer: InodePointer
  val fs: FileSystem
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit]
  
  def mode: Int
  def mode_=(m: Int): Future[Unit]
  
  def uid: Int
  def uid_=(u: Int): Future[Unit]
  /*
  def gid: Int
  def gid_=(g: Int): Future[Unit]
  
  def ctime: Timespec
  def ctime_=(ts: Timespec): Future[Unit]
  
  def mtime: Timespec
  def mtime_=(ts: Timespec): Future[Unit]
  
  def atime: Timespec
  def atime_=(ts: Timespec): Future[Unit]
  * 
  */
}