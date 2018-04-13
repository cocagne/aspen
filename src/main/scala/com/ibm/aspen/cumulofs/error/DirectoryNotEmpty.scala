package com.ibm.aspen.cumulofs.error

import com.ibm.aspen.cumulofs.DirectoryPointer

case class DirectoryNotEmpty(pointer: DirectoryPointer) extends CumuloFSError