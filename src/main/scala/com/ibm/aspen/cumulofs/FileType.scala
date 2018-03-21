package com.ibm.aspen.cumulofs

object FileType extends Enumeration {
  import FileMode._
  
  val File            = Value("File")
  val Directory       = Value("Directory")
  val Symlink         = Value("Symlink")
  val UnixSocket      = Value("UnixSocket")
  val CharacterDevice = Value("CharaceterDevice")
  val BlockDevice     = Value("BlockDevice")
  val FIFO            = Value("FIFO")
  val Unknown         = Value("Unknown") // Error type
  
  def fromMode(mode: Int): Value = (mode & S_IFMT) match {
    case S_IFSOCK => UnixSocket
    case S_IFLNK  => Symlink
    case S_IFREG  => File
    case S_IFBLK  => BlockDevice
    case S_IFDIR  => Directory
    case S_IFCHR  => CharacterDevice
    case S_IFFIFO => FIFO
    case _        => Unknown
  }
}