package com.ibm.aspen.amorfs

trait InodeCache {
  
  def lookup(inodeNumber: Long): Option[InodePointer]
  
  def drop(inodeNumber: Long): Unit
}