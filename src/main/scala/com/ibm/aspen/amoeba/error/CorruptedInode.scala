package com.ibm.aspen.amoeba.error

import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.amoeba.InodePointer

/** Thrown when loading an inode that does not contain all of the required keys for that inode type */
case class CorruptedInode(pointer: InodePointer, content: Map[Key, Array[Byte]]) extends AmoebaError