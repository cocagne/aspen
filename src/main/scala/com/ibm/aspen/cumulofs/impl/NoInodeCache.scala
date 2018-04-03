package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.InodeCache
import com.ibm.aspen.cumulofs.InodePointer

class NoInodeCache extends InodeCache {
  def lookup(inodeNumber: Long): Option[InodePointer] = None
  
  def drop(inodeNumber: Long): Unit = ()
}