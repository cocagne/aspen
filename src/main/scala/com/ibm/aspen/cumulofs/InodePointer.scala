package com.ibm.aspen.cumulofs

import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.ObjectPointer
import java.nio.ByteBuffer

sealed abstract class InodePointer {
  
  val number: Long
  val pointer: KeyValueObjectPointer
  val ftype: FileType.Value 
  
  def encodedSize: Int = 8 + 1 + pointer.encodedSize
  
  def toArray: Array[Byte] = {
    val arr = new Array[Byte](encodedSize)
    encodeInto(ByteBuffer.wrap(arr))
    arr
  }
  
  def encodeInto(bb: ByteBuffer): Unit = {
    bb.put(FileType.toByte(ftype))
    bb.putLong(number)
    pointer.encodeInto(bb)
  }
}
                
class FilePointer(val number: Long, val pointer: KeyValueObjectPointer) extends InodePointer {
  val ftype = FileType.File
}

class DirectoryPointer(val number: Long, val pointer: KeyValueObjectPointer) extends InodePointer {
  val ftype = FileType.Directory
}

class SymlinkPointer(val number: Long, val pointer: KeyValueObjectPointer) extends InodePointer {
  val ftype = FileType.Symlink
}

class UnixSocketPointer(val number: Long, val pointer: KeyValueObjectPointer) extends InodePointer {
  val ftype = FileType.UnixSocket
}

class CharacterDevicePointer(val number: Long, val pointer: KeyValueObjectPointer) extends InodePointer {
  val ftype = FileType.UnixSocket
}

class BlockDevicePointer(val number: Long, val pointer: KeyValueObjectPointer) extends InodePointer {
  val ftype = FileType.BlockDevice
}

class FIFOPointer(val number: Long, val pointer: KeyValueObjectPointer) extends InodePointer {
  val ftype = FileType.FIFO
}

object InodePointer {
  def apply(ftype: FileType.Value, number: Long, pointer: KeyValueObjectPointer): InodePointer = ftype match {
    case FileType.File            => new FilePointer(number, pointer) 
    case FileType.Directory       => new DirectoryPointer(number, pointer) 
    case FileType.Symlink         => new SymlinkPointer(number, pointer) 
    case FileType.UnixSocket      => new UnixSocketPointer(number, pointer) 
    case FileType.CharacterDevice => new CharacterDevicePointer(number, pointer) 
    case FileType.BlockDevice     => new BlockDevicePointer(number, pointer) 
    case FileType.FIFO            => new FIFOPointer(number, pointer) 
  }
  
  def apply(arr: Array[Byte]): InodePointer = apply(ByteBuffer.wrap(arr), None)
  
  /** If size is None, the end of the array marks the end of the pointer */
  def apply(arr: Array[Byte], size: Option[Int]): InodePointer = apply(ByteBuffer.wrap(arr), size)
  
  /** If size is None, the limit of the byte buffer marks the end of the pointer */
  def apply(bb: ByteBuffer, size: Option[Int]=None): InodePointer = {
    val ftype = FileType.fromByte(bb.get())
    val number = bb.getLong()
    val pointer = KeyValueObjectPointer(bb, size)
    apply(ftype, number, pointer)
  }
}