package com.ibm.aspen.cumulofs

import com.ibm.aspen.base.AspenSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import com.ibm.aspen.core.read.CorruptedObject
import com.ibm.aspen.cumulofs.error.InvalidInode

trait InodeLoader {
  val system: AspenSystem
  
  val inodeTable: InodeTable
  
  val inodeCache: InodeCache
  
  def load(pointer: DirectoryPointer)(implicit ec: ExecutionContext): Future[DirectoryInode] = {
    iload(pointer).map(i => i.asInstanceOf[DirectoryInode])
  }
  def load(pointer: FilePointer)(implicit ec: ExecutionContext): Future[FileInode] = {
    iload(pointer).map(i => i.asInstanceOf[FileInode])
  }
  def load(pointer: SymlinkPointer)(implicit ec: ExecutionContext): Future[SymlinkInode] = {
    iload(pointer).map(i => i.asInstanceOf[SymlinkInode])
  }
  def load(pointer: UnixSocketPointer)(implicit ec: ExecutionContext): Future[UnixSocketInode] = {
    iload(pointer).map(i => i.asInstanceOf[UnixSocketInode])
  }
  def load(pointer: FIFOPointer)(implicit ec: ExecutionContext): Future[FIFOInode] = {
    iload(pointer).map(i => i.asInstanceOf[FIFOInode])
  }
  def load(pointer: CharacterDevicePointer)(implicit ec: ExecutionContext): Future[CharacterDeviceInode] = {
    iload(pointer).map(i => i.asInstanceOf[CharacterDeviceInode])
  }
  def load(pointer: BlockDevicePointer)(implicit ec: ExecutionContext): Future[BlockDeviceInode] = {
    iload(pointer).map(i => i.asInstanceOf[BlockDeviceInode])
  }
  
  def load(inodeNumber: Long)(implicit ec: ExecutionContext): Future[Inode] = {
    val opointer = inodeCache.lookup(inodeNumber) match {
      case Some(ptr) => Future.successful(Some(ptr))
      case None => system.retryStrategy.retryUntilSuccessful {
        inodeTable.lookup(inodeNumber)
      }
    }
    
    opointer.flatMap { o => o match {
      case None => Future.failed(InvalidInode(inodeNumber))
      case Some(iptr) => iload(iptr) 
    }}
  }
  
  def iload(pointer: InodePointer)(implicit ec: ExecutionContext): Future[Inode] = {
    val pload = Promise[Inode]()
      
    system.readObject(pointer.pointer) onComplete {
      case Success(kvos) => pload.success(Inode(pointer, kvos.revision, kvos.refcount, kvos.contents)) 
        
        
      case Failure(e: CorruptedObject) =>
        // TODO: If pointer fails to read, we'll need to read the inode table (could be stale pointer). If new pointer
        //       for that inode exists, we'll need to use a callback function to update the stale pointer
        pload.failure(e)
        
      case Failure(cause) => pload.failure(cause)
    }
    
    pload.future
  }

}