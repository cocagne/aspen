package com.ibm.aspen.cumulofs

trait InodeCache {
  
  def lookup(inodeNumber: Long): Option[InodePointer]
  
  def drop(inodeNumber: Long): Unit
}