package com.ibm.aspen.amorfs.nfs

import com.ibm.aspen.amorfs._
import org.dcache.nfs.vfs.Stat

import scala.concurrent.ExecutionContext

abstract class NFSBaseFile(implicit val ec: ExecutionContext) {
  val file: BaseFile

  def pointer: InodePointer = file.pointer
  def fs: FileSystem = file.fs

  def refresh(): Unit = blockingCall(file.refresh())

  def mode: Int = file.mode
  def uid: Int = file.uid
  def gid: Int = file.gid
  def ctime: Timespec = file.ctime
  def mtime: Timespec = file.mtime
  def atime: Timespec = file.atime
  def links: Int = file.links

  def setMode(newMode: Int): Unit = blockingCall(file.setMode(newMode))

  def setUID(uid: Int): Unit = blockingCall(file.setUID(uid))

  def setGID(gid: Int): Unit = blockingCall(file.setGID(gid))

  def setCtime(ts: Timespec): Unit = blockingCall(file.setCtime(ts))

  def setMtime(ts: Timespec): Unit = blockingCall(file.setMtime(ts))

  def setAtime(ts: Timespec): Unit = blockingCall(file.setAtime(ts))

  def flush(): Unit = blockingCall(file.flush())

  def setattr(
               newUID: Int,
               newGID: Int,
               ctime: Timespec,
               mtime: Timespec,
               atime: Timespec,
               newMode: Int): Unit = blockingCall(file.setattr(newUID, newGID, ctime, mtime, atime, newMode))

  def freeResources(): Unit = blockingCall(file.freeResources())


  def nfsFileType: Int = FileType.toMode(pointer.ftype)

  def nfsStat: Stat = {
    val stats: Stat = new Stat

    stats.setMode(mode)
    stats.setNlink(links)
    stats.setGeneration(0L)
    stats.setIno(pointer.number.asInstanceOf[Int])
    stats.setSize(0)
    stats.setATime(atime.millis)
    stats.setMTime(mtime.millis)
    stats.setCTime(ctime.millis)
    stats.setFileid(pointer.number.asInstanceOf[Int])
    stats.setUid(uid)
    stats.setGid(gid)

    stats
  }
}
