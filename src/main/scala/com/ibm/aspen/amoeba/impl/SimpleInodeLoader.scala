package com.ibm.aspen.amoeba.impl

import com.ibm.aspen.amoeba.InodeTable
import com.ibm.aspen.amoeba.InodeCache
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.amoeba.InodeLoader

class SimpleInodeLoader(
    val system: AspenSystem,
    val inodeTable: InodeTable,
    val inodeCache: InodeCache) extends InodeLoader {
  
}