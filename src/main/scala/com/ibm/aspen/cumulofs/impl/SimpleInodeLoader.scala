package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.InodeTable
import com.ibm.aspen.cumulofs.InodeCache
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.cumulofs.InodeLoader

class SimpleInodeLoader(
    val system: AspenSystem,
    val inodeTable: InodeTable,
    val inodeCache: InodeCache) extends InodeLoader {
  
}