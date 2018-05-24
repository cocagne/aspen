package com.ibm.aspen.cumulofs

import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.HLCTimestamp
import java.nio.charset.StandardCharsets
import com.ibm.aspen.base.tieredlist.TieredKeyValueList
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.cumulofs.error.CorruptedInode
import com.ibm.aspen.util.Varint
import com.ibm.aspen.base.Transaction

object Inode {
  
  // --------- Required ---------
  val ModeKey        = Key(0)  // Int
  val UIDKey         = Key(1)  // Int
  val GIDKey         = Key(2)  // Int
  val CtimeKey       = Key(3)  // Timespec(seconds: Long, nanoseconds: Long)
  
  val RequiredKeys = Set(ModeKey, UIDKey, GIDKey, CtimeKey)
  
  // --------- Optional ---------
  val MtimeKey            = Key(4)  // Timespec(seconds: Long, nanoseconds: Long) 
  val AtimeKey            = Key(5)  // Timespec(seconds: Long, nanoseconds: Long) 
  
  val XAttrsKey           = Key(6)  // List of <varint key-len><varint value-len><key><value>
  val XAttrsTieredListKey = Key(7)  // TieredList of key-value pairs
  
  val KeysReserved        = 100     // Keys Reserved for future use
  
  
  def getInitialContent(mode: Int, uid: Int, gid: Int, content:List[(Key, Array[Byte])]): (List[KeyValueOperation], Map[Key,Value]) = {
    val icontent = 
        (ModeKey,  int2arr(mode)) ::
        (UIDKey,   int2arr(uid)) ::
        (GIDKey,   int2arr(gid)) ::
        (CtimeKey, Timespec.now.toArray) :: content
    
    val ts = HLCTimestamp.now
    val opsList = KeyValueOperation.insertOperations(icontent, ts)
    val kvmap = icontent.foldLeft(Map[Key,Value]())((m, t) => m + (t._1 -> Value(t._1, t._2, ts)))
    
    (opsList, kvmap)
  }
  
  def apply(
      pointer: InodePointer, 
      revision: ObjectRevision,
      refcount: ObjectRefcount,
      timestamp: HLCTimestamp,
      content: Map[Key, Value]): Inode = pointer match {
    case p: FilePointer            => new FileInode(p, revision, refcount, timestamp, content)
    case p: DirectoryPointer       => new DirectoryInode(p, revision, refcount, timestamp, content)
    case p: SymlinkPointer         => new SymlinkInode(p, revision, refcount, timestamp, content)
    case p: UnixSocketPointer      => new UnixSocketInode(p, revision, refcount, timestamp, content)
    case p: CharacterDevicePointer => new CharacterDeviceInode(p, revision, refcount, timestamp, content)
    case p: BlockDevicePointer     => new BlockDeviceInode(p, revision, refcount, timestamp, content)
    case p: FIFOPointer            => new FIFOInode(p, revision, refcount, timestamp, content)
  }
  
  def setMode(pointer: InodePointer, newMode: Int)(implicit tx: Transaction): (Key, Value) = {
    (ModeKey, Value(ModeKey, int2arr((newMode & ~FileMode.S_IFMT) | FileType.toMode(pointer.ftype)), tx.timestamp))
  }
  
  def setUID(newUID: Int)(implicit tx: Transaction): (Key, Value) = (UIDKey, Value(UIDKey, int2arr(newUID), tx.timestamp))
   
  def setGID(newGID: Int)(implicit tx: Transaction): (Key, Value) = (GIDKey, Value(GIDKey, int2arr(newGID), tx.timestamp))
  
  def setCtime(ctime: Timespec)(implicit tx: Transaction): (Key, Value) = (CtimeKey, Value(CtimeKey, ctime.toArray, tx.timestamp))
  
  def setMtime(mtime: Timespec)(implicit tx: Transaction): (Key, Value) = (MtimeKey, Value(MtimeKey, mtime.toArray, tx.timestamp))
  
  def setAtime(atime: Timespec)(implicit tx: Transaction): (Key, Value) = (AtimeKey, Value(AtimeKey, atime.toArray, tx.timestamp))
  
  def setattr(
      inode: Inode, 
      newUID: Int, 
      newGID: Int, 
      ctime: Timespec, 
      mtime: Timespec, 
      atime: Timespec, 
      newMode: Int)(implicit tx: Transaction): Map[Key, Value] = {
    inode.content + setUID(newUID) + setGID(newGID) + setCtime(ctime) + setMtime(mtime) + setAtime(atime) + setMode(inode.pointer, newMode)
  }
}

sealed abstract class Inode {
    
  val pointer: InodePointer
  val revision: ObjectRevision
  val refcount: ObjectRefcount
  val content: Map[Key, Value]
  val timestamp: HLCTimestamp
  
  import Inode._
  
  def requiredKeys: Set[Key] = Inode.RequiredKeys

  if (!requiredKeys.subsetOf(content.keySet))
    throw CorruptedInode(pointer, content)
  
  def mode: Int = (arr2int(content(ModeKey).value) & ~FileMode.S_IFMT) | FileType.toMode(pointer.ftype)
  def uid: Int = arr2int(content(UIDKey).value)
  def gid: Int = arr2int(content(GIDKey).value)
  def ctime: Timespec = Timespec(content(CtimeKey).value)
  def mtime: Timespec = content.get(MtimeKey) match {
    case None => ctime
    case Some(v) => Timespec(v.value)
  }
  def atime: Timespec = content.get(AtimeKey) match {
    case None => mtime
    case Some(v) => Timespec(v.value)
  }
  
}

// ----- Directory -----

object DirectoryInode {
  val ParentDirectoryInodePointerKey = Key(20) // EncodedInodePointer
  val ContentTieredListKey           = Key(21) // TieredList of (filename -> EncodedInodePointer)

  def getInitialContent(mode: Int, uid: Int, gid: Int, parentDirectoryInodePointer: Option[DirectoryPointer]): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFDIR 
    val icontent = parentDirectoryInodePointer match {
      case Some(p) => (ParentDirectoryInodePointerKey -> p.toArray) :: Nil
      case None => Nil
    }
    Inode.getInitialContent(m, uid, gid, icontent)
  }
}

class DirectoryInode(
    val pointer: DirectoryPointer, 
    val revision: ObjectRevision,
    val refcount: ObjectRefcount,
    val timestamp: HLCTimestamp,
    val content: Map[Key, Value]) extends Inode {
 
  import DirectoryInode._
  
  def parentDirectoryPointer: Option[DirectoryPointer] = content.get(ParentDirectoryInodePointerKey).map { v =>
    InodePointer(v.value).asInstanceOf[DirectoryPointer]
  }
  
  def hasContentTree: Boolean = content.contains(ContentTieredListKey)
  
  def contentTree: Option[TieredKeyValueList.Root] = content.get(ContentTieredListKey) map { value =>
    TieredKeyValueList.Root(value.value)
  }
}

// ----- File -----

object FileInode {
  val FileSizeKey        = Key(20) // Varint
  val FileIndexRootKey   = Key(21) // Serialized pointer to root of the file index tree + tier level
    
  def getInitialContent(mode: Int, uid: Int, gid: Int): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFREG
    Inode.getInitialContent(m, uid, gid, Nil)
  }
}

class FileInode(
    val pointer: FilePointer, 
    val revision: ObjectRevision,
    val refcount: ObjectRefcount,
    val timestamp: HLCTimestamp,
    val content: Map[Key, Value]) extends Inode {
 
  import FileInode._
  
  def size: Long = content.get(FileSizeKey) match {
    case None => 0
    case Some(varr) => Varint.getUnsignedLong(varr.value)
  }
  
  def fileIndexRoot(): Option[Array[Byte]] = content.get(FileIndexRootKey) match {
    case None => None
    case Some(v) => Some(v.value)
  }
  
  def update(
      newRevision: ObjectRevision, 
      newTimestamp: HLCTimestamp, 
      newSize: Long=size, 
      newFileIndexRoot:Option[Array[Byte]]=None, 
      newRefcount:ObjectRefcount=refcount): FileInode = {
    val newContent = newFileIndexRoot match {
      case None => content + (FileSizeKey -> Value(FileSizeKey, Varint.unsignedLongToArray(newSize), newTimestamp))
      case Some(arr) => content + (FileSizeKey -> Value(FileSizeKey, Varint.unsignedLongToArray(newSize), newTimestamp)) +
        (FileIndexRootKey -> Value(FileIndexRootKey, arr, newTimestamp))
    }
    new FileInode(pointer, newRevision, newRefcount, newTimestamp, newContent) 
  }
}

// ----- Symlink -----

object SymlinkInode {
  val LinkKey = Key(20)
  
  val RequiredKeys = Inode.RequiredKeys ++ Set(LinkKey)
  
  def getInitialContent(mode: Int, uid: Int, gid: Int, link: String): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFLNK
    Inode.getInitialContent(m, uid, gid, (LinkKey -> link.getBytes(StandardCharsets.UTF_8)) :: Nil)
  }
  
  def setLink(newLink: String)(implicit tx: Transaction): (Key, Value) = (LinkKey, Value(LinkKey, newLink.getBytes(StandardCharsets.UTF_8), tx.timestamp))
}

class SymlinkInode(
    val pointer: SymlinkPointer, 
    val revision: ObjectRevision,
    val refcount: ObjectRefcount,
    val timestamp: HLCTimestamp,
    val content: Map[Key, Value]) extends Inode {
 
  import SymlinkInode._
  
  override def requiredKeys = RequiredKeys
  
  def size: Int = content(LinkKey).value.length
  
  def link: String = new String(content(LinkKey).value, StandardCharsets.UTF_8)
}

// ----- Unix Socket -----

object UnixSocketInode {
  def getInitialContent(mode: Int, uid: Int, gid: Int): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFSOCK
    Inode.getInitialContent(m, uid, gid, Nil)
  }
}

class UnixSocketInode(
    val pointer: UnixSocketPointer, 
    val revision: ObjectRevision,
    val refcount: ObjectRefcount,
    val timestamp: HLCTimestamp,
    val content: Map[Key, Value]) extends Inode {
 
  import UnixSocketInode._
}

// ----- FIFO -----

object FIFOInode {
  def getInitialContent(mode: Int, uid: Int, gid: Int): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFFIFO
    Inode.getInitialContent(m, uid, gid, Nil)
  }
}

class FIFOInode(
    val pointer: FIFOPointer, 
    val revision: ObjectRevision,
    val refcount: ObjectRefcount,
    val timestamp: HLCTimestamp,
    val content: Map[Key, Value]) extends Inode {
 
  import FIFOInode._
}

// ----- Devices -----

object DeviceInode {
  val DeviceTypeKey = Key(20)  // Device major/minor types
  
  val RequiredKeys = Inode.RequiredKeys ++ Set(DeviceInode.DeviceTypeKey)
  
  def getInitialContent(mode: Int, uid: Int, gid: Int, rdev: Int): (List[KeyValueOperation], Map[Key,Value]) = {
    Inode.getInitialContent(mode, uid, gid, (DeviceTypeKey -> int2arr(rdev)) :: Nil)
  }
  
  def setDeviceType(rdev: Int)(implicit tx: Transaction): (Key, Value) = (DeviceTypeKey, Value(DeviceTypeKey, int2arr(rdev), tx.timestamp))
}

sealed abstract class DeviceInode extends Inode {
  def rdev: Int = arr2int(content(DeviceInode.DeviceTypeKey).value)
}

object CharacterDeviceInode {
  def getInitialContent(mode: Int, uid: Int, gid: Int, rdev: Int): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFCHR
    DeviceInode.getInitialContent(m, uid, gid, rdev)
  }
}

class CharacterDeviceInode(
    val pointer: CharacterDevicePointer, 
    val revision: ObjectRevision,
    val refcount: ObjectRefcount,
    val timestamp: HLCTimestamp,
    val content: Map[Key, Value]) extends DeviceInode {
 
  import CharacterDeviceInode._
  
  override def requiredKeys = DeviceInode.RequiredKeys
}

object BlockDeviceInode {
  def getInitialContent(mode: Int, uid: Int, gid: Int, rdev: Int): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFBLK
    DeviceInode.getInitialContent(m, uid, gid, rdev)
  }
}

class BlockDeviceInode(
    val pointer: BlockDevicePointer, 
    val revision: ObjectRevision,
    val refcount: ObjectRefcount,
    val timestamp: HLCTimestamp,
    val content: Map[Key, Value]) extends DeviceInode {
 
  import BlockDeviceInode._
  
  override def requiredKeys = DeviceInode.RequiredKeys
}



