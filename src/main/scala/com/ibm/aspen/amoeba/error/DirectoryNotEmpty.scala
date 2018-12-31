package com.ibm.aspen.amoeba.error

import com.ibm.aspen.amoeba.DirectoryPointer

case class DirectoryNotEmpty(pointer: DirectoryPointer) extends AmoebaError