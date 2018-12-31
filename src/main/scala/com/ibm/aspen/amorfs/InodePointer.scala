package com.ibm.aspen.amorfs

import com.ibm.aspen.core.objects.DataObjectPointer
import java.nio.ByteBuffer
import java.util.UUID

sealed abstract class InodePointer {
  
  val number: Long
  val pointer: DataObjectPointer
  val ftype: FileType.Value

  def encodedSize: Int = 8 + 1 + pointer.encodedSize

  def uuid: UUID = pointer.uuid

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

class FilePointer(val number: Long, val pointer: DataObjectPointer) extends InodePointer {
  val ftype = FileType.File
}

class DirectoryPointer(val number: Long, val pointer: DataObjectPointer) extends InodePointer {
  val ftype = FileType.Directory
}

class SymlinkPointer(val number: Long, val pointer: DataObjectPointer) extends InodePointer {
  val ftype = FileType.Symlink
}

class UnixSocketPointer(val number: Long, val pointer: DataObjectPointer) extends InodePointer {
  val ftype = FileType.UnixSocket
}

class CharacterDevicePointer(val number: Long, val pointer: DataObjectPointer) extends InodePointer {
  val ftype = FileType.CharacterDevice
}

class BlockDevicePointer(val number: Long, val pointer: DataObjectPointer) extends InodePointer {
  val ftype = FileType.BlockDevice
}

class FIFOPointer(val number: Long, val pointer: DataObjectPointer) extends InodePointer {
  val ftype = FileType.FIFO
}

object InodePointer {
  def apply(ftype: FileType.Value, number: Long, pointer: DataObjectPointer): InodePointer = ftype match {
    case FileType.File            => new FilePointer(number, pointer)
    case FileType.Directory       => new DirectoryPointer(number, pointer)
    case FileType.Symlink         => new SymlinkPointer(number, pointer)
    case FileType.UnixSocket      => new UnixSocketPointer(number, pointer)
    case FileType.CharacterDevice => new CharacterDevicePointer(number, pointer)
    case FileType.BlockDevice     => new BlockDevicePointer(number, pointer)
    case FileType.FIFO            => new FIFOPointer(number, pointer)
  }

  def apply(arr: Array[Byte]): InodePointer = apply(ByteBuffer.wrap(arr))

  /** If size is None, the limit of the byte buffer marks the end of the pointer */
  def apply(bb: ByteBuffer): InodePointer = {
    val ftype = FileType.fromByte(bb.get())
    val number = bb.getLong()
    val pointer = DataObjectPointer(bb)
    apply(ftype, number, pointer)
  }
}