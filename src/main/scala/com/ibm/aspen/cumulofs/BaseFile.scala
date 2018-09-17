package com.ibm.aspen.cumulofs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value

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
  def links: Int
 
  def setMode(newMode: Int)(implicit ec: ExecutionContext): Future[Unit]
  
  def setUID(uid: Int)(implicit ec: ExecutionContext): Future[Unit]
  
  def setGID(gid: Int)(implicit ec: ExecutionContext): Future[Unit]
  
  def setCtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit]
  
  def setMtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit]
  
  def setAtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit]

  def flush()(implicit ec: ExecutionContext): Future[Unit]

  def prepareHardLink()(implicit tx: Transaction, ec: ExecutionContext): Unit

  def setattr(
      newUID: Int, 
      newGID: Int, 
      ctime: Timespec, 
      mtime: Timespec, 
      atime: Timespec, 
      newMode: Int)(implicit ec: ExecutionContext): Future[Unit]
  
  /** Frees all objects owned by the inode */
  def freeResources()(implicit ec: ExecutionContext): Future[Unit] = Future.unit
}