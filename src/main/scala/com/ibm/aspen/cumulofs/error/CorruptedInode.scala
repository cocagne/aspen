package com.ibm.aspen.cumulofs.error

import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.cumulofs.InodePointer

/** Thrown when loading an inode that does not contain all of the required keys for that inode type */
case class CorruptedInode(pointer: InodePointer, content: Map[Key,Value]) extends CumuloFSError 