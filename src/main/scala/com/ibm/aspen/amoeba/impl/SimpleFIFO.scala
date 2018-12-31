package com.ibm.aspen.amoeba.impl

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.amoeba.{FIFO, FIFOInode, FIFOPointer, FileSystem}

class SimpleFIFO(override val pointer: FIFOPointer,
                 initialInode: FIFOInode,
                 revision: ObjectRevision,
                 fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with FIFO {

  override def inode: FIFOInode = super.inode.asInstanceOf[FIFOInode]
}