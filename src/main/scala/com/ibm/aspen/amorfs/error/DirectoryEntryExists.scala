package com.ibm.aspen.amorfs.error

import com.ibm.aspen.amorfs.DirectoryPointer

case class DirectoryEntryExists(pointer: DirectoryPointer, name: String) extends AmorfsError