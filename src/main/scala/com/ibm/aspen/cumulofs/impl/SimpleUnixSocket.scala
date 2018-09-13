package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.cumulofs.{FileSystem, UnixSocket, UnixSocketInode, UnixSocketPointer}

class SimpleUnixSocket(override val pointer: UnixSocketPointer,
                       initialInode: UnixSocketInode,
                       revision: ObjectRevision,
                       fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with UnixSocket {

  override def inode: UnixSocketInode = super.inode.asInstanceOf[UnixSocketInode]
}