package com.ibm.aspen.cumulofs

import com.ibm.aspen.base.Transaction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation

object InodeTable {
  val NullInode = 0L
  val RootInode = 1L
}

trait InodeTable {
  
  /** Future completes when the transaction is ready for commit */
  def prepareInodeAllocation(ftype: FileType.Value, inodeOps: List[KeyValueOperation])(implicit tx: Transaction, ec: ExecutionContext): Future[InodePointer]
  
  /** Removes the Inode from the table. This method does NOT decrement the reference count on the Inode object. */
  def delete(pointer: InodePointer)(implicit ec: ExecutionContext): Future[Unit]
  
  def lookup(inodeNumber: Long)(implicit ec: ExecutionContext): Future[Option[InodePointer]]

  def lookupRoot()(implicit ec: ExecutionContext): Future[DirectoryPointer] = lookup(InodeTable.RootInode).map(_.get.asInstanceOf[DirectoryPointer])
}