package com.ibm.aspen.cumulofs

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

trait BaseFile {
  
  def cachedInode: Inode
  
  /** Re-reads the inode and returns a fresh copy. This method also updates the 
   *  cachedInode member variable
   */
  def refreshInode()(implicit ec: ExecutionContext): Future[Inode]
  
}