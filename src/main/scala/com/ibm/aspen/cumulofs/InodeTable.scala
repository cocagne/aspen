package com.ibm.aspen.cumulofs

import com.ibm.aspen.base.Transaction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait InodeTable {
  
  /** Future completes when the transaction is ready for commit */
  def prepareAllocation(inode: InodePointer)(implicit tx: Transaction, ec: ExecutionContext): Future[InodeNumber]
  
  def lookup(inodeNumber: InodeNumber)(implicit ec: ExecutionContext): Future[Option[InodePointer]]
  
}