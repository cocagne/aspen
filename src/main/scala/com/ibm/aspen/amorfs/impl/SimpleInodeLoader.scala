package com.ibm.aspen.amorfs.impl

import com.ibm.aspen.amorfs.InodeTable
import com.ibm.aspen.amorfs.InodeCache
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.amorfs.InodeLoader

class SimpleInodeLoader(
    val system: AspenSystem,
    val inodeTable: InodeTable,
    val inodeCache: InodeCache) extends InodeLoader {
  
}