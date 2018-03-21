package com.ibm.aspen.cumulofs

import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.HLCTimestamp

object Inode {
  
  val ModeKey        = Key(0)  // Int
  val UIDKey         = Key(1)  // Int
  val GIDKey         = Key(2)  // Int
  val CtimeKey       = Key(3)  // Timespec(seconds: Long, nanoseconds: Long)
  val MtimeKey       = Key(4)  // Timespec(seconds: Long, nanoseconds: Long) or missing
  val AtimeKey       = Key(5)  // Timespec(seconds: Long, nanoseconds: Long) or missing
  val SizeKey        = Key(6)  // Long
  val EmbeddedXAttrs = Key(7)  // List <varint-key-len><varint-value-len><key><value>
  val ExternalXAttrs = Key(8)  // TieredList of key-value pairs
  val DeviceType     = Key(9)  // Device major/minor types
  val EmbeddedData   = Key(10) // Array[Byte]
  val ExternalData   = Key(11) // TieredList
  
  def getInitialInodeContent(mode: Int, uid: Int, gid: Int): (List[KeyValueOperation], Map[Key,Value]) = {
    require(FileType.fromMode(mode) != FileType.Unknown)
    
    val content = List(
        (ModeKey,  int2arr(mode)),
        (UIDKey,   int2arr(uid)),
        (GIDKey,   int2arr(gid)),
        (CtimeKey, Timespec.now.toArray))
    
    val ts = HLCTimestamp.now
    val opsList = KeyValueOperation.insertOperations(content, ts)
    val kvmap = content.foldLeft(Map[Key,Value]())((m, t) => m + (t._1 -> Value(t._1, t._2, ts)))
    
    (opsList, kvmap)
  }
  
  def apply(
      pointer: InodePointer, 
      revision: ObjectRevision, 
      content: Map[Key, Value]): Inode = new Inode(pointer, revision, content)
}

class Inode(
    val pointer: InodePointer,
    val revision: ObjectRevision,
    val content: Map[Key, Value]) {
  
  import Inode._
  
  def fileType: FileType.Value = FileType.fromMode(mode)
  def mode: Int = arr2int(content(ModeKey).value)
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
