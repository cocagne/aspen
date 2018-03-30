package com.ibm.aspen.cumulofs

import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.HLCTimestamp
import java.nio.charset.StandardCharsets

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
  val SizeKey             = Key(6)  // Long
  
  val XAttrsKey           = Key(7)  // List of <varint key-len><varint value-len><key><value>
  val XAttrsTieredListKey = Key(8)  // TieredList of key-value pairs
  
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
      content: Map[Key, Value]): Inode = pointer match {
    case p: FilePointer            => new FileInode(p, revision, content)
    case p: DirectoryPointer       => new DirectoryInode(p, revision, content)
    case p: SymlinkPointer         => new SymlinkInode(p, revision, content)
    case p: UnixSocketPointer      => new UnixSocketInode(p, revision, content)
    case p: CharacterDevicePointer => new CharacterDeviceInode(p, revision, content)
    case p: BlockDevicePointer     => new BlockDeviceInode(p, revision, content)
    case p: FIFOPointer            => new FIFOInode(p, revision, content)
  }
}

sealed abstract class Inode {
    
  val pointer: InodePointer
  val revision: ObjectRevision
  val content: Map[Key, Value] 
  
  val RequiredKeys: Set[Key]
  
  import Inode._
  
  if (!RequiredKeys.subsetOf(content.keySet))
    throw CorruptedInode(pointer, content)
  
  def mode: Int = (arr2int(content(ModeKey).value) & ~FileMode.S_IFMT) & FileType.toMode(pointer.ftype)
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
  def size: Long = content.get(SizeKey) match {
    case None => 0
    case Some(v) => arr2long(v.value)
  }
}

// ----- Directory -----

object DirectoryInode {
  val ParentDirectoryInodeKey = Key(20)
  val ContentTieredListKey    = Key(21) // TieredList of (filename -> EncodedInodePointer)
  
  def getInitialContent(mode: Int, uid: Int, gid: Int, parentDirectoryInode: Long): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFDIR 
    Inode.getInitialContent(m, uid, gid, (ParentDirectoryInodeKey -> long2arr(parentDirectoryInode)) :: Nil)
  }
}

class DirectoryInode(
    val pointer: DirectoryPointer, 
    val revision: ObjectRevision, 
    val content: Map[Key, Value]) extends Inode {
 
  import DirectoryInode._
  
  val RequiredKeys = Inode.RequiredKeys ++ Set(ParentDirectoryInodeKey)
  
  def parentDirectoryInode: Long = arr2long(content(ParentDirectoryInodeKey).value)
}

// ----- File -----

object FileInode {
  val InitialSegmentsKey  = Key(20) // Pointers to the first few data objects. List <varint-offset><varint-pointer-len><pointer>
  val DataTieredListKey   = Key(21) // TieredList
    
  def getInitialContent(mode: Int, uid: Int, gid: Int): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFREG
    Inode.getInitialContent(m, uid, gid, Nil)
  } 
}

class FileInode(
    val pointer: FilePointer, 
    val revision: ObjectRevision, 
    val content: Map[Key, Value]) extends Inode {
 
  import FileInode._
 
  val RequiredKeys = Inode.RequiredKeys
}

// ----- Symlink -----

object SymlinkInode {
  val LinkKey = Key(20)
  
  def getInitialContent(mode: Int, uid: Int, gid: Int, link: String): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFLNK
    Inode.getInitialContent(m, uid, gid, (LinkKey -> link.getBytes(StandardCharsets.UTF_8)) :: Nil)
  }
}

class SymlinkInode(
    val pointer: SymlinkPointer, 
    val revision: ObjectRevision, 
    val content: Map[Key, Value]) extends Inode {
 
  import SymlinkInode._
  
  val RequiredKeys = Inode.RequiredKeys ++ Set(LinkKey)
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
    val content: Map[Key, Value]) extends Inode {
 
  import UnixSocketInode._
 
  val RequiredKeys = Inode.RequiredKeys
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
    val content: Map[Key, Value]) extends Inode {
 
  import FIFOInode._
  
  val RequiredKeys = Inode.RequiredKeys
}

// ----- Devices -----

object DeviceInode {
  val DeviceTypeKey = Key(20)  // Device major/minor types
  
  def getInitialContent(mode: Int, uid: Int, gid: Int, rdev: Int): (List[KeyValueOperation], Map[Key,Value]) = {
    Inode.getInitialContent(mode, uid, gid, (DeviceTypeKey -> int2arr(rdev)) :: Nil)
  }
}

sealed abstract class DeviceInode extends Inode {}

object CharacterDeviceInode {
  def getInitialContent(mode: Int, uid: Int, gid: Int, rdev: Int): (List[KeyValueOperation], Map[Key,Value]) = {
    val m = (mode & ~FileMode.S_IFMT) | FileMode.S_IFCHR
    DeviceInode.getInitialContent(m, uid, gid, rdev)
  }
}

class CharacterDeviceInode(
    val pointer: CharacterDevicePointer, 
    val revision: ObjectRevision, 
    val content: Map[Key, Value]) extends DeviceInode {
 
  import CharacterDeviceInode._
  
  val RequiredKeys = Inode.RequiredKeys ++ Set(DeviceInode.DeviceTypeKey)
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
    val content: Map[Key, Value]) extends DeviceInode {
 
  import BlockDeviceInode._
  
  val RequiredKeys = Inode.RequiredKeys ++ Set(DeviceInode.DeviceTypeKey)
}



