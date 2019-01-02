package com.ibm.aspen.amoeba

import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.ObjectRevision

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import com.ibm.aspen.core.read.{CorruptedObject, InvalidObject}
import com.ibm.aspen.amoeba.error.InvalidInode
import org.apache.logging.log4j.scala.Logging

trait InodeLoader extends Logging {
  val system: AspenSystem
  
  val inodeTable: InodeTable
  
  def load(inodeNumber: Long)(implicit ec: ExecutionContext): Future[(Inode, ObjectRevision)] = {
    inodeTable.lookup(inodeNumber).flatMap {
      case None => Future.failed(InvalidInode(inodeNumber))
      case Some(iptr) => load(iptr)
    }
  }
  
  def load(pointer: InodePointer)(implicit ec: ExecutionContext): Future[(Inode, ObjectRevision)] = {

    val pload = Promise[(Inode, ObjectRevision)]()

    system.readObject(pointer.pointer) onComplete {
      case Success(dos) =>
        pload.success((Inode(pointer, dos.data), dos.revision))

      case Failure(_: InvalidObject) =>
        // Probably deleted
        pload.failure(InvalidInode(pointer.number))

      case Failure(e: CorruptedObject) =>
        // TODO: If pointer fails to read, we'll need to read the inode table (could be stale pointer). If new pointer
        //       for that inode exists, we'll need to use a callback function to update the stale pointer
        pload.failure(e)
        logger.error(s"Corrupted Inode: $pointer")
        
      case Failure(cause) =>
        pload.failure(cause)
        logger.error(s"Unexpected error encountered during load of inode $pointer, $cause")
    }
    
    pload.future
  }

}