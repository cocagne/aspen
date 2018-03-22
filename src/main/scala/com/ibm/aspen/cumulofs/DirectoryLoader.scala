package com.ibm.aspen.cumulofs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait DirectoryLoader {
  def load(inode: InodeNumber, pointer: InodePointer)(implicit ec: ExecutionContext): Future[Directory]
}