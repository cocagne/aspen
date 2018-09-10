package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.cumulofs.{FileSystem, UnixSocket, UnixSocketInode, UnixSocketPointer}

class SimpleUnixSocket(override val pointer: UnixSocketPointer,
                       override protected var cachedInode: UnixSocketInode,
                       revision: ObjectRevision,
                       fs: FileSystem) extends SimpleBaseFile(pointer, revision, cachedInode, fs) with UnixSocket {
}