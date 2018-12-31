package com.ibm.aspen.amoeba.impl

import com.ibm.aspen.amoeba.InodeCache
import com.ibm.aspen.amoeba.InodePointer

class NoInodeCache extends InodeCache {
  def lookup(inodeNumber: Long): Option[InodePointer] = None
  
  def drop(inodeNumber: Long): Unit = ()
}