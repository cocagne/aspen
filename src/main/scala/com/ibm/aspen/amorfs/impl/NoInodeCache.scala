package com.ibm.aspen.amorfs.impl

import com.ibm.aspen.amorfs.InodeCache
import com.ibm.aspen.amorfs.InodePointer

class NoInodeCache extends InodeCache {
  def lookup(inodeNumber: Long): Option[InodePointer] = None
  
  def drop(inodeNumber: Long): Unit = ()
}