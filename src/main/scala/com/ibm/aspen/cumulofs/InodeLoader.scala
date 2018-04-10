package com.ibm.aspen.cumulofs

import com.ibm.aspen.base.AspenSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import com.ibm.aspen.core.read.CorruptedObject

trait InodeLoader {
  val system: AspenSystem
  
  val inodeTable: InodeTable
  
  val inodeCache: InodeCache
  
  def loadDirectory(pointer: DirectoryPointer)(implicit ec: ExecutionContext): Future[DirectoryInode] = {
    load(pointer).map(i => i.asInstanceOf[DirectoryInode])
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
      case Some(iptr) => load(iptr) 
    }}
  }
  
  def load(pointer: InodePointer)(implicit ec: ExecutionContext): Future[Inode] = {
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