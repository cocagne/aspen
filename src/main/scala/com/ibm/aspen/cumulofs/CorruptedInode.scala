package com.ibm.aspen.cumulofs

import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value

case class CorruptedInode(pointer: InodePointer, content: Map[Key,Value]) extends CumuloFSError 