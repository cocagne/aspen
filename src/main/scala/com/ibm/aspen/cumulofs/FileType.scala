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
  
  
  
  def fromMode(mode: Int): Value = (mode & S_IFMT) match {
    case S_IFSOCK => UnixSocket
    case S_IFLNK  => Symlink
    case S_IFREG  => File
    case S_IFBLK  => BlockDevice
    case S_IFDIR  => Directory
    case S_IFCHR  => CharacterDevice
    case S_IFFIFO => FIFO
    case _        => throw InvalidPointer((mode & S_IFMT).asInstanceOf[Byte])
  }
  
  def toMode(value: Value): Int = value match {
    case File            => S_IFREG
    case Directory       => S_IFDIR 
    case Symlink         => S_IFLNK 
    case UnixSocket      => S_IFSOCK 
    case CharacterDevice => S_IFCHR 
    case BlockDevice     => S_IFBLK 
    case FIFO            => S_IFFIFO
  }
   
  
  def toByte(value: Value): Byte = {
    val i = value match {
      case File            => 0
      case Directory       => 1
      case Symlink         => 2
      case UnixSocket      => 3
      case CharacterDevice => 4
      case BlockDevice     => 5
      case FIFO            => 6
    }
    
    i.asInstanceOf[Byte]
  }
  
  def fromByte(b: Byte): Value = b match {
    case 0 => File
    case 1 => Directory      
    case 2 => Symlink        
    case 3 => UnixSocket     
    case 4 => CharacterDevice
    case 5 => BlockDevice    
    case 6 => FIFO           
    case _ => throw InvalidPointer(b)        
  }
}