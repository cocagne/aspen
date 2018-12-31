package com.ibm.aspen.cumulofs.nfs

import com.ibm.aspen.base.{AspenSystem, StopRetrying, Transaction}
import com.ibm.aspen.core.read.FatalReadError
import com.ibm.aspen.cumulofs._
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

  private def retryUntilSuccessfulOr[T](prepare: Transaction => Future[T])
                                                (checkForErrors: => Future[Unit])
                                                (implicit ec: ExecutionContext): T = blockingCall {
    def onFail(err: Throwable): Future[Unit] = err match {
      case err: FatalReadError => throw StopRetrying(err)
      case err =>
        logger.info(s"retryUntilSuccessOr error $err")
        file.refresh().map(_ => checkForErrors)
    }
    file.fs.system.transactUntilSuccessfulWithRecovery(onFail) { tx =>
      prepare(tx)
    }
  }

  private def retryCreationOr[T](prepare: Transaction => Future[Future[T]])
                                (checkForErrors: => Future[Unit])
                                (implicit ec: ExecutionContext): T = blockingCall {
    val p = Promise[T]()

    def onFail(err: Throwable): Future[Unit] = err match {
      case err: FatalReadError => throw StopRetrying(err)
      case _ => file.refresh().map(_ => checkForErrors)
    }

    val fcreate = file.fs.system.transactUntilSuccessfulWithRecovery(onFail) { tx =>
      prepare(tx)
    }

    fcreate.foreach { ft =>
      ft.foreach { t =>
        p.success(t)
      }
    }

    fcreate.failed.foreach { err =>
      p.failure(err)
    }

    p.future
  }

  def insert(name: String, pointer: InodePointer, incref: Boolean=true): Unit = {
    retryUntilSuccessfulOr { implicit tx =>
      file.prepareInsert(name, pointer, incref)
    }{
      file.getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(new ExistException(s"$name already exists"))
      }
    }
  }


  def delete(name: String, decref: Boolean=true): Unit = {
    retryUntilSuccessfulOr { implicit tx =>
      file.prepareDelete(name, decref).map(_ => ())
    }{
      file.getEntry(name).map {
        case None =>  throw StopRetrying(new NoEntException(s"$name does not exist"))
        case Some(_) =>
      }
    }
  }

  def rename(oldName: String, newName: String): Unit = {
    retryUntilSuccessfulOr { implicit tx =>
      file.prepareRename(oldName, newName)
    }{
      Future.sequence(file.getEntry(oldName) :: file.getEntry(newName) :: Nil).map { lst =>
        if (lst.head.isEmpty)
          throw StopRetrying(new NoEntException(s"$oldName does not exist"))

        if (lst.tail.head.nonEmpty)
          throw StopRetrying(new ExistException(s"$newName already exists"))
      }
    }
  }

  def hardLink(name: String, f: BaseFile): Unit = {
    retryUntilSuccessfulOr { implicit tx =>
      file.hardLink(name, f)
    }{
      file.getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(new ExistException(s"$name already exists"))
      }
    }
  }

  def createDirectory(name: String, mode: Int, uid: Int, gid: Int): DirectoryPointer = {
    retryCreationOr { implicit tx =>
      file.prepareCreateDirectory(name, mode, uid, gid)
    }{
      file.getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(new ExistException(s"$name already exists"))
      }
    }
  }

  def createFile(name: String, mode: Int, uid: Int, gid: Int): FilePointer = {
    retryCreationOr { implicit tx =>
      file.prepareCreateFile(name, mode, uid, gid)
    }{
      file.getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(new ExistException(s"$name already exists"))
      }
    }
  }

  def createSymlink(name: String, mode: Int, uid: Int, gid: Int, link: String): SymlinkPointer = {
    retryCreationOr { implicit tx =>
      file.prepareCreateSymlink(name, mode, uid, gid, link)
    }{
      file.getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(new ExistException(s"$name already exists"))
      }
    }
  }

  def createUnixSocket(name: String, mode: Int, uid: Int, gid: Int): UnixSocketPointer = {
    retryCreationOr { implicit tx =>
      file.prepareCreateUnixSocket(name, mode, uid, gid)
    }{
      file.getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(new ExistException(s"$name already exists"))
      }
    }
  }

  def createFIFO(name: String, mode: Int, uid: Int, gid: Int): FIFOPointer = {
    retryCreationOr { implicit tx =>
      file.prepareCreateFIFO(name, mode, uid, gid)
    }{
      file.getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(new ExistException(s"$name already exists"))
      }
    }
  }

  def createCharacterDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int): CharacterDevicePointer = {
    retryCreationOr { implicit tx =>
      file.prepareCreateCharacterDevice(name, mode, uid, gid, rdev)
    }{
      file.getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(new ExistException(s"$name already exists"))
      }
    }
  }

  def createBlockDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int): BlockDevicePointer = {
    retryCreationOr { implicit tx =>
      file.prepareCreateBlockDevice(name, mode, uid, gid, rdev)
    }{
      file.getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(new ExistException(s"$name already exists"))
      }
    }
  }
}
