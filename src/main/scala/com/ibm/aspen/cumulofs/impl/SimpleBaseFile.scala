package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.base.{StopRetrying, Transaction}
import com.ibm.aspen.core.objects.{DataObjectPointer, ObjectRevision}
import com.ibm.aspen.cumulofs._

import scala.concurrent.{ExecutionContext, Future, Promise}

object SimpleBaseFile {

  trait FileOperation {
    /** This promise completes after the file has been successfully updated and all internal data
      * structures (such as the FileIndex) have been updated to reflect the new file state.
      */
    val promise: Promise[Inode] = Promise[Inode]()

    def result: Future[Inode] = promise.future

    def prepareTransaction(pointer: DataObjectPointer,
                           revision: ObjectRevision,
                           inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit]
  }

  abstract class SimpleSet extends FileOperation {

    def update(inode: Inode): Inode

    def prepareTransaction(pointer: DataObjectPointer,
                           revision: ObjectRevision,
                           inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {

      val updatedInode = update(inode)

      tx.overwrite(pointer, revision, updatedInode.toDataBuffer)

      tx.result.foreach(_ => promise.success(updatedInode))

      Future.unit
    }
  }

  case class SetUID(uid: Int) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(uid = Some(uid), inodeNumber = None)
  }

  case class SetGID(gid: Int) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(gid = Some(gid), inodeNumber = None)
  }

  case class SetLinks(links: Int) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(links = Some(links), inodeNumber = None)
  }

  case class SetMode(newMode: Int) extends SimpleSet {
    private val maskedMode = newMode & ~FileMode.S_IFMT
    def update(inode: Inode): Inode = inode.update(mode = Some((inode.mode & FileMode.S_IFMT) | maskedMode), inodeNumber = None)
  }

  case class SetCtime(ts: Timespec) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(ctime = Some(ts), inodeNumber = None)
  }

  case class SetMtime(ts: Timespec) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(mtime = Some(ts), inodeNumber = None)
  }

  case class SetAtime(ts: Timespec) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(atime = Some(ts), inodeNumber = None)
  }

  case class SetAttr(uid: Int, gid: Int, ct: Timespec, mt: Timespec, at: Timespec, mode: Int)
    extends SimpleSet {

    private val maskedMode = mode & ~FileMode.S_IFMT

    def update(inode: Inode): Inode = {
      inode.update(mode = Some((inode.mode & FileMode.S_IFMT) | maskedMode), uid = Some(uid), gid = Some(gid), ctime = Some(ct), mtime = Some(mt), atime = Some(at), inodeNumber = None)
    }
  }
}

/** Provides a simple mechanism for satisfying file modification operations across all file types.
  *
  * All updates to a file involve updating the mtime. To minimize conflicts, we'll use a per-file singleton object that multiple
  * file handles can refer to. All modification options are serialized by the File interface (internally maintains a concurrent
  * linked list of requested operations).
  *
  * All file operations are functions called to accomplish the specific requested task and return a Future to the result.
  * E.g. setMtime(newMtime), setCtime(newCtime) Base inode trait has common methods for doing the basic metadata updates across
  * all file types. Requires maintaining an internal queue of operations to prevent self-contention
  */
abstract class SimpleBaseFile(val pointer: InodePointer,
                              protected var cachedInodeRevision: ObjectRevision,
                              protected var cachedInode: Inode,
                              val fs: FileSystem) extends BaseFile {

  import SimpleBaseFile._

  private[this] val pendingOps = new java.util.concurrent.ConcurrentLinkedQueue[FileOperation]()
  private[this] var activeOp: Option[FileOperation] = None

  def inode: Inode = synchronized { cachedInode }

  protected def inodeState: (Inode, ObjectRevision) = synchronized {(cachedInode, cachedInodeRevision)}

  protected def setCachedInode(newInode: Inode, newRevision:ObjectRevision): Unit = synchronized {
    cachedInode = newInode
    cachedInodeRevision = newRevision
  }

  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    fs.inodeLoader.iload(pointer).map(t => setCachedInode(t._1, t._2))
  }

  def mode: Int = inode.mode
  def uid: Int = inode.uid
  def gid: Int = inode.gid
  def links: Int = inode.links
  def ctime: Timespec = inode.ctime
  def mtime: Timespec = inode.mtime
  def atime: Timespec = inode.atime


  def setMode(newMode: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetMode(newMode))
  def setUID(uid: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetUID(uid))
  def setGID(gid: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetGID(gid))
  def setLinks(links: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetLinks(links))
  def setCtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetCtime(ts))
  def setMtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetMtime(ts))
  def setAtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetAtime(ts))

  def setattr(
      newUID: Int,
      newGID: Int,
      ctime: Timespec,
      mtime: Timespec,
      atime: Timespec,
      newMode: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetAttr(newUID, newGID, ctime, mtime, atime, newMode))

  protected def enqueueOp(op: FileOperation)(implicit ec: ExecutionContext): Future[Unit] = {
    pendingOps.add(op)
    beginNextOp()
    op.result.map(_=>())
  }

  private[this] def beginNextOp()(implicit ec: ExecutionContext): Unit = synchronized {
    activeOp match {
      case Some(_) =>
      case None =>
        val next = pendingOps.poll()
        if (next != null) {
          //println(s"Begining Inode operation on ${inode.pointer.pointer.uuid} with revision ${inode.revision}")
          activeOp = Some(next)
          executeOp(next)
        }
    }
  }

  private[this] def executeOp(op: FileOperation)(implicit ec: ExecutionContext): Unit = {

    def onCommitFailure(foo: Throwable): Future[Unit] = {
      refresh().recover {
        case err => throw StopRetrying(err) // Only InvalidObject should cause a read failure, which means the object has been deleted
      }.map(_=>())
    }

    def attempt(): Future[(ObjectRevision, Inode)] = {
      implicit val tx: Transaction = fs.system.newTransaction()

      val (ainode, arevision) = inodeState
      
      val fresult = for {
        _<- op.prepareTransaction(pointer.pointer, arevision, ainode)
        _<-tx.commit()
        updatedInode <- op.result
      } yield (tx.txRevision, updatedInode)
      
      fresult.failed.foreach(err => tx.invalidateTransaction(err))
      
      fresult
    }

    var retryCount = 0
    fs.system.retryStrategy.retryUntilSuccessful(onCommitFailure _) {
      retryCount += 1
      if (retryCount < 5) {
        
        attempt() map { t => synchronized {
          
          val (newRevision, updatedInode) = t
          //println(s"Updated Inode ${inode.pointer.pointer.uuid} revision ${inode.revision} to $newRevision")
          setCachedInode(updatedInode, newRevision)
          activeOp = None
          op.promise.success(updatedInode)
          beginNextOp()
        }}
      } else {
        println(s"****** ERROR 5 retryCount limit reached for file operation $op on inode object ${pointer.pointer.uuid}")
        Future.unit
      }
    }.failed.foreach {
      // Propagate critical failures to the caller (attempted operation on deleted inode)
      cause => op.promise.failure(cause)
    }
  }
}