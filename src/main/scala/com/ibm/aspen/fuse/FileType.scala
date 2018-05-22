package com.ibm.aspen.fuse

object FileType extends Enumeration {
  val UnixSocket      = Value("UnixSocket")
  val SymbolicLink    = Value("SymbolicLink")
  val RegularFile     = Value("RegularFile")
  val BlockDevice     = Value("BlockDevice")
  val Directory       = Value("Directory")
  val CharacterDevice = Value("CharacterDevice")
  val Fifo            = Value("Fifo")
}
