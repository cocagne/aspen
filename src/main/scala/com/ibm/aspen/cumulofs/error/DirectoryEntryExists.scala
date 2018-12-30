package com.ibm.aspen.cumulofs.error

import com.ibm.aspen.cumulofs.DirectoryPointer

case class DirectoryEntryExists(pointer: DirectoryPointer, name: String) extends CumuloFSError