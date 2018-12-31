package com.ibm.aspen.amoeba.error

import com.ibm.aspen.amoeba.DirectoryPointer

case class DirectoryEntryExists(pointer: DirectoryPointer, name: String) extends AmoebaError