package com.ibm.aspen.cumulofs.nfs

import com.ibm.aspen.cumulofs._
import org.dcache.nfs.status.NoEntException

import scala.concurrent.ExecutionContext

class NFSDirectory(val file: Directory)(implicit ec: ExecutionContext) extends NFSBaseFile {

  def lookup(name: String): Option[InodePointer] = blockingCall(file.lookup(name))

  def getContents(): List[DirectoryEntry] = blockingCall(file.getContents())

  def parentInodeNumber: Long = file.inode.oparent match {
    case None => throw new NoEntException()
    case Some(ptr) => ptr.number
  }

  def getEntry(name: String): Option[InodePointer] = blockingCall(file.getEntry(name))

  def insert(name: String, pointer: InodePointer, incref: Boolean=true): Unit = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.prepareInsert(name, pointer, incref)
    }
  }

  def delete(name: String, decref: Boolean=true): Unit = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.prepareDelete(name, decref)
    }
  }

  def rename(oldName: String, newName: String): Unit = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.prepareRename(oldName, newName)
    }
  }

  def hardLink(name: String, f: BaseFile): Unit = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.hardLink(name, f)
    }
  }

  def createDirectory(name: String, mode: Int, uid: Int, gid: Int): DirectoryPointer = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.createDirectory(name, mode, uid, gid)
    }
  }

  def createFile(name: String, mode: Int, uid: Int, gid: Int): FilePointer = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.createFile(name, mode, uid, gid)
    }
  }

  def createSymlink(name: String, mode: Int, uid: Int, gid: Int, link: String): SymlinkPointer = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.createSymlink(name, mode, uid, gid, link)
    }
  }

  def createUnixSocket(name: String, mode: Int, uid: Int, gid: Int): UnixSocketPointer = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.createUnixSocket(name, mode, uid, gid)
    }
  }

  def createFIFO(name: String, mode: Int, uid: Int, gid: Int): FIFOPointer = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.createFIFO(name, mode, uid, gid)
    }
  }

  def createCharacterDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int): CharacterDevicePointer = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.createCharacterDevice(name, mode, uid, gid, rdev)
    }
  }

  def createBlockDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int): BlockDevicePointer = {
    retryTransactionUntilSuccessful(file.fs.system) { implicit tx =>
      file.createBlockDevice(name, mode, uid, gid, rdev)
    }
  }
}
