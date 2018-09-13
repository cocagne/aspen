package com.ibm.aspen.cumulofs

import java.nio.charset.StandardCharsets

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait Symlink extends BaseFile {
  val pointer: SymlinkPointer
  
  def size: Int
  
  def symLink: Array[Byte]
  
  def setSymLink(newLink: Array[Byte])(implicit ec: ExecutionContext): Future[Unit]

  def symLinkAsString: String = new String(symLink, StandardCharsets.UTF_8)
}