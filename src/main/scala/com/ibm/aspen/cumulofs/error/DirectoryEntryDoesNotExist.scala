package com.ibm.aspen.cumulofs.error

import com.ibm.aspen.cumulofs.DirectoryPointer

class DirectoryEntryDoesNotExist(pointer: DirectoryPointer, name: String) extends CumuloFSError