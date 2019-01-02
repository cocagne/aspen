package com.ibm.aspen.amoeba.impl

import com.ibm.aspen.amoeba.{InodeLoader, InodeTable}
import com.ibm.aspen.base.AspenSystem

class SimpleInodeLoader(
    val system: AspenSystem,
    val inodeTable: InodeTable) extends InodeLoader {
  
}