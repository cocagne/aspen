package com.ibm.aspen.amoeba.error

import com.ibm.aspen.amoeba.DirectoryPointer

case class DirectoryEntryDoesNotExist(pointer: DirectoryPointer, name: String) extends AmoebaError {
  override def toString: String = s"$name does not exist in directory ${pointer.uuid}"
}