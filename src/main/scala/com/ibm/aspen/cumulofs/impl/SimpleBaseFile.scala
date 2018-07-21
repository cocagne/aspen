package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.InodePointer
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.BaseFile
import com.ibm.aspen.cumulofs.Inode
import com.ibm.aspen.base.Transaction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import scala.concurrent.Promise
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import scala.util.Success
import scala.util.Failure
import com.ibm.aspen.base.StopRetrying
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.cumulofs.Timespec
import com.ibm.aspen.core.objects.ObjectRefcount

object SimpleBaseFile {
  
  /** fileStateUpdated future completes after successful commit and after any internal data
   *  structures (such as a FileIndex) have been updated to reflect the state of the new change.
   *  The future value is the updated Inode content
   */
  case class OpResult(readyToCommit: Future[Unit], fileStateUpdated: Future[Map[Key,Value]], newRefcount: Option[ObjectRefcount]=None)
  
  trait FileOperation {
    val promise = Promise[Unit]()
    def result = promise.future
    def attempt(inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): OpResult
  }
  
  abstract class SimpleSet extends FileOperation {
    //val promise = Promise[Unit]()
    
    def getUpdate(inode: Inode)(implicit tx: Transaction): (Key,Value)
    
    def attempt(inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): OpResult = {
      val updatedContent = inode.content + getUpdate(inode)
      tx.overwrite(inode.pointer.pointer, inode.revision, Nil, KeyValueOperation.contentToOps(updatedContent))
      OpResult(Future.successful(()), Future.successful(updatedContent))
    }
  }
  
  case class SetUID(uid: Int) extends SimpleSet {
    def getUpdate(inode: Inode)(implicit tx: Transaction): (Key,Value) = Inode.setUID(uid) 
  }
  
  case class SetGID(gid: Int) extends SimpleSet {
    def getUpdate(inode: Inode)(implicit tx: Transaction): (Key,Value) = Inode.setGID(gid)
  }
  
  case class SetMode(pointer: InodePointer, newMode: Int) extends SimpleSet {
    def getUpdate(inode: Inode)(implicit tx: Transaction): (Key,Value) = Inode.setMode(pointer, newMode)
  }
  
  case class SetCtime(ts: Timespec) extends SimpleSet {
    def getUpdate(inode: Inode)(implicit tx: Transaction): (Key,Value) = Inode.setCtime(ts)
  }
  
  case class SetMtime(ts: Timespec) extends SimpleSet {
    def getUpdate(inode: Inode)(implicit tx: Transaction): (Key,Value) = Inode.setMtime(ts)
  }
  
  case class SetAtime(ts: Timespec) extends SimpleSet {
    def getUpdate(inode: Inode)(implicit tx: Transaction): (Key,Value) = Inode.setAtime(ts)
  }
  
  case class SetAttr(uid: Int, gid: Int, ct: Timespec, mt: Timespec, at: Timespec, mode: Int) extends FileOperation {
    def attempt(inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): OpResult = {
      val updatedContent = Inode.setattr(inode, uid, gid, ct, mt, at, mode)
      tx.overwrite(inode.pointer.pointer, inode.revision, Nil, KeyValueOperation.contentToOps(updatedContent))
      OpResult(Future.successful(()), Future.successful(updatedContent))
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
abstract class SimpleBaseFile(val fs: FileSystem) extends BaseFile {
  
  import SimpleBaseFile._
  
  private[this] val pendingOps = new java.util.concurrent.ConcurrentLinkedQueue[FileOperation]()
  private[this] var activeOp: Option[FileOperation] = None
  
  protected def cachedInode: Inode
  
  def mode: Int = synchronized { cachedInode.mode }
  def uid: Int = synchronized { cachedInode.uid }
  def gid: Int = synchronized { cachedInode.gid }
  def ctime: Timespec = synchronized { cachedInode.ctime }
  def mtime: Timespec = synchronized { cachedInode.mtime }
  def atime: Timespec = synchronized { cachedInode.atime }
  
  def linkCount: Int = synchronized { cachedInode.refcount.count - 1 }
 
  def setMode(newMode: Int)(implicit ec: ExecutionContext): Future[Unit] = {
    val iptr = synchronized { cachedInode.pointer }
    enqueueOp(SetMode(pointer, newMode))
  }
  
  def setUID(uid: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetUID(uid))
  
  def setGID(gid: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetGID(gid))
  
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
    op.result
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
        case err => throw new StopRetrying(err) // Only InvalidObject should cause a read failure, which means the object has been deleted
      }.map(_=>())
    }
    
    def attempt(): Future[(ObjectRevision, HLCTimestamp, Map[Key,Value], Option[ObjectRefcount])] = {
      implicit val tx = fs.system.newTransaction()
      
      val icopy = synchronized { cachedInode }
      
      val r = op.attempt(icopy)
      
      val fresult = for {
        prepared <- r.readyToCommit
        _<-tx.commit()
        updatedState <- r.fileStateUpdated
      } yield (tx.txRevision, tx.timestamp, updatedState, r.newRefcount)
      
      fresult.failed.foreach(err => tx.invalidateTransaction(err))
      
      fresult
    }
    var retryCount = 0
    fs.system.retryStrategy.retryUntilSuccessful(onCommitFailure _) {
      retryCount += 1
      if (retryCount < 5) {
        
        attempt() map { t => synchronized {
          
          val (newRevision, newTimestamp, updatedState, newRefcount) = t
          //println(s"Updated Inode ${inode.pointer.pointer.uuid} revision ${inode.revision} to $newRevision")
          updateInode(newRevision, newTimestamp, updatedState, newRefcount)
          activeOp = None
          op.promise.success(())
          beginNextOp()
        }}
      } else {
        Future.unit
      }
    }.failed.foreach {
      // Propagate critical failures to the caller (attempted operation on deleted inode)
      cause => op.promise.failure(cause)
    }
  }
}