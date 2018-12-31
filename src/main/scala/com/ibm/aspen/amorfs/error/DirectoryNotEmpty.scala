package com.ibm.aspen.amorfs.error

import com.ibm.aspen.amorfs.DirectoryPointer

case class DirectoryNotEmpty(pointer: DirectoryPointer) extends AmorfsError