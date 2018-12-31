package com.ibm.aspen.amorfs.error

import com.ibm.aspen.amorfs.DirectoryPointer

class DirectoryEntryDoesNotExist(pointer: DirectoryPointer, name: String) extends AmorfsError