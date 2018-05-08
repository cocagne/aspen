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

object SimpleBaseFile {
  
  /** fileStateUpdated future completes after successful commit and after any internal data
   *  structures (such as a FileIndex) have been updated to reflect the state of the new change.
   *  The future value is the updated Inode content
   */
  case class OpResult(readyToCommit: Future[Unit], fileStateUpdated: Future[Map[Key,Value]])
  
  trait FileOperation {
    val promise: Promise[Unit]
    def result = promise.future
    def attempt(inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): OpResult
  }
  
  abstract class SimpleSet extends FileOperation {
    val promise = Promise[Unit]()
    
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
}

abstract class SimpleBaseFile(val fs: FileSystem) extends BaseFile {
  
  import SimpleBaseFile._
  
  private[this] val pendingOps = new java.util.concurrent.ConcurrentLinkedQueue[FileOperation]()
  private[this] var activeOp: Option[FileOperation] = None
  
  protected def inode: Inode
  protected def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value]): Unit
  
  def mode: Int = synchronized { inode.mode }
  def uid: Int = synchronized { inode.uid }
  def gid: Int = synchronized { inode.gid }
  def ctime: Timespec = synchronized { inode.ctime }
  def mtime: Timespec = synchronized { inode.mtime }
  def atime: Timespec = synchronized { inode.atime }
 
  def setMode(newMode: Int)(implicit ec: ExecutionContext): Future[Unit] = {
    val iptr = synchronized { inode.pointer }
    enqueueOp(SetMode(pointer, newMode))
  }
  
  def setUID(uid: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetUID(uid))
  
  def setGID(gid: Int)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetGID(gid))
  
  def setCtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetCtime(ts))
  
  def setMtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetMtime(ts))
  
  def setAtime(ts: Timespec)(implicit ec: ExecutionContext): Future[Unit] = enqueueOp(SetAtime(ts))

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
    
    def attempt(): Future[(ObjectRevision, HLCTimestamp, Map[Key,Value])] = {
      implicit val tx = fs.system.newTransaction()
      
      val icopy = synchronized { inode }
      
      val r = op.attempt(icopy)
      
      val fresult = for {
        prepared <- r.readyToCommit
        _<-tx.commit()
        updatedState <- r.fileStateUpdated
      } yield (tx.txRevision, tx.timestamp, updatedState)
      
      fresult.failed.foreach(err => tx.invalidateTransaction(err))
      
      fresult
    }
    
    fs.system.retryStrategy.retryUntilSuccessful(onCommitFailure _) {
      attempt() map { t => synchronized {
        val (newRevision, newTimestamp, updatedState) = t
        updateInode(newRevision, newTimestamp, updatedState)
        activeOp = None
        op.promise.success(())
        beginNextOp()
      }}
    }.failed.foreach {
      // Propagate critical failures to the caller (attempted operation on deleted inode)
      cause => op.promise.failure(cause)
    }
  }
}