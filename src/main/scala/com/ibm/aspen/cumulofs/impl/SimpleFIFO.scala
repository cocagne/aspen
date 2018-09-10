package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.cumulofs.{FIFO, FIFOInode, FIFOPointer, FileSystem}

class SimpleFIFO(override val pointer: FIFOPointer,
                 override protected var cachedInode: FIFOInode,
                 revision: ObjectRevision,
                 fs: FileSystem) extends SimpleBaseFile(pointer, revision, cachedInode, fs) with FIFO {
}