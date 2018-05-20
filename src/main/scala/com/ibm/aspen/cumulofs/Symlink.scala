package com.ibm.aspen.cumulofs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait Symlink extends BaseFile {
  val pointer: SymlinkPointer
  
  def size: Int
  
  def link: String
  
  def setLink(newLink: String)(implicit ec: ExecutionContext): Future[Unit]
}