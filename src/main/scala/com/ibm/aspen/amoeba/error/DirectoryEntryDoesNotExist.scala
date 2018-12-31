package com.ibm.aspen.amoeba.error

import com.ibm.aspen.amoeba.DirectoryPointer

class DirectoryEntryDoesNotExist(pointer: DirectoryPointer, name: String) extends AmoebaError