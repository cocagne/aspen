package com.ibm.aspen.amoeba

trait InodeCache {
  
  def lookup(inodeNumber: Long): Option[InodePointer]
  
  def drop(inodeNumber: Long): Unit
}