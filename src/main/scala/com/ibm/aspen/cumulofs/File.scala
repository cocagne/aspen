package com.ibm.aspen.cumulofs

trait File extends BaseFile {
  val pointer: FilePointer
  
  def inode: FileInode

  def size: Long = inode.size

}