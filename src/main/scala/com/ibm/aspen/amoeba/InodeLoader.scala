package com.ibm.aspen.amoeba

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.ObjectRevision

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import com.ibm.aspen.core.read.CorruptedObject
import com.ibm.aspen.amoeba.error.InvalidInode

trait InodeLoader {
  val system: AspenSystem
  
  val inodeTable: InodeTable
  
  val inodeCache: InodeCache
  
  def load(pointer: DirectoryPointer)(implicit ec: ExecutionContext): Future[(DirectoryInode, ObjectRevision)] = {
    iload(pointer).map(i => (i._1.asInstanceOf[DirectoryInode], i._2))
  }
  def load(pointer: FilePointer)(implicit ec: ExecutionContext): Future[(FileInode, ObjectRevision)] = {
    iload(pointer).map(i => (i._1.asInstanceOf[FileInode], i._2))
  }
  def load(pointer: SymlinkPointer)(implicit ec: ExecutionContext): Future[(SymlinkInode, ObjectRevision)] = {
    iload(pointer).map(i => (i._1.asInstanceOf[SymlinkInode], i._2))
  }
  def load(pointer: UnixSocketPointer)(implicit ec: ExecutionContext): Future[(UnixSocketInode, ObjectRevision)] = {
    iload(pointer).map(i => (i._1.asInstanceOf[UnixSocketInode], i._2))
  }
  def load(pointer: FIFOPointer)(implicit ec: ExecutionContext): Future[(FIFOInode, ObjectRevision)] = {
    iload(pointer).map(i => (i._1.asInstanceOf[FIFOInode], i._2))
  }
  def load(pointer: CharacterDevicePointer)(implicit ec: ExecutionContext): Future[(CharacterDeviceInode, ObjectRevision)] = {
    iload(pointer).map(i => (i._1.asInstanceOf[CharacterDeviceInode], i._2))
  }
  def load(pointer: BlockDevicePointer)(implicit ec: ExecutionContext): Future[(BlockDeviceInode, ObjectRevision)] = {
    iload(pointer).map(i => (i._1.asInstanceOf[BlockDeviceInode], i._2))
  }
  
  def load(inodeNumber: Long)(implicit ec: ExecutionContext): Future[(Inode, ObjectRevision)] = {
    val opointer = inodeCache.lookup(inodeNumber) match {
      case Some(ptr) => Future.successful(Some(ptr))
      case None => system.retryStrategy.retryUntilSuccessful {
        inodeTable.lookup(inodeNumber)
      }
    }
    
    opointer.flatMap {
      case None => Future.failed(InvalidInode(inodeNumber))
      case Some(iptr) => iload(iptr)
    }
  }
  
  def iload(pointer: InodePointer)(implicit ec: ExecutionContext): Future[(Inode, ObjectRevision)] = {

    val pload = Promise[(Inode, ObjectRevision)]()

    system.readObject(pointer.pointer) onComplete {
      case Success(dos) =>
        pload.success((Inode(pointer, dos.data), dos.revision))

      case Failure(e: CorruptedObject) =>
        // TODO: If pointer fails to read, we'll need to read the inode table (could be stale pointer). If new pointer
        //       for that inode exists, we'll need to use a callback function to update the stale pointer
        pload.failure(e)
        
      case Failure(cause) =>
        pload.failure(cause)
    }
    
    pload.future
  }

}