package com.ibm.aspen.amoeba.nfs

import com.ibm.aspen.base.{AspenSystem, StopRetrying, Transaction}
import com.ibm.aspen.core.read.FatalReadError
import com.ibm.aspen.amoeba._
import org.apache.logging.log4j.scala.Logging
import org.dcache.nfs.status.{ExistException, NoEntException}

import scala.concurrent.{ExecutionContext, Future, Promise}

class NFSDirectory(val file: Directory)(implicit ec: ExecutionContext) extends NFSBaseFile with Logging {

  def lookup(name: String): Option[InodePointer] = blockingCall(file.lookup(name))

  def getContents(): List[DirectoryEntry] = blockingCall(file.getContents())

  def parentInodeNumber: Long = file.inode.oparent match {
    case None => throw new NoEntException()
    case Some(ptr) => ptr.number
  }

  def getEntry(name: String): Option[InodePointer] = blockingCall(file.getEntry(name))

  def delete(name: String, decref: Boolean=true): Unit = blockingCall { file.delete(name, decref) }


  def rename(oldName: String, newName: String): Unit = blockingCall { file.rename(oldName, newName) }

  def hardLink(name: String, f: BaseFile): Unit =  blockingCall { file.hardLink(name, f) }

  def createDirectory(name: String, mode: Int, uid: Int, gid: Int): DirectoryPointer =  blockingCall { file.createDirectory(name, mode, uid, gid) }

  def createFile(name: String, mode: Int, uid: Int, gid: Int): FilePointer =  blockingCall { file.createFile(name, mode, uid, gid) }

  def createSymlink(name: String, mode: Int, uid: Int, gid: Int, link: String): SymlinkPointer = blockingCall { file.createSymlink(name, mode, uid, gid, link) }

  def createUnixSocket(name: String, mode: Int, uid: Int, gid: Int): UnixSocketPointer = blockingCall { file.createUnixSocket(name, mode, uid, gid) }

  def createFIFO(name: String, mode: Int, uid: Int, gid: Int): FIFOPointer = blockingCall { file.createFIFO(name, mode, uid, gid) }

  def createCharacterDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int): CharacterDevicePointer = blockingCall { file.createCharacterDevice(name, mode, uid, gid, rdev) }

  def createBlockDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int): BlockDevicePointer = blockingCall { file.createBlockDevice(name, mode, uid, gid, rdev) }
}
