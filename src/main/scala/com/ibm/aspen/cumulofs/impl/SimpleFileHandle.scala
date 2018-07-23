package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.FileHandle
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.FilePointer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import java.nio.ByteBuffer
import com.ibm.aspen.cumulofs.File
import com.ibm.aspen.core.DataBuffer
import scala.concurrent.Promise
import scala.collection.immutable.Queue
import com.ibm.aspen.core.read.CorruptedObject
import com.ibm.aspen.base.StopRetrying
import com.ibm.aspen.core.read.InvalidObject
import com.ibm.aspen.cumulofs.FileInode
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.util.Varint
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.cumulofs.Inode
import com.ibm.aspen.cumulofs.Timespec

object SimpleFileHandle {
  
  def bufferedSize(l: List[DataBuffer]): Int = l.foldLeft(0)((sz, db) => sz + db.size)
  
  private sealed abstract class WriteOperation(val completePromise: Promise[Unit])
  
  private class Truncate(completePromise: Promise[Unit], val newFileEnd: Long) extends WriteOperation(completePromise)
  
  private class Write(completePromise: Promise[Unit], val startOffset: Long, val buffers: List[DataBuffer]) extends WriteOperation(completePromise)
  
  private class Flush(completePromise: Promise[Unit]) extends WriteOperation(completePromise)
  
  private class WriteQueue {
    
    private sealed abstract class QueueEntry {
      val completePromise = Promise[Unit]()
    }
    
    private class PendingTruncation(val newFileEnd: Long) extends QueueEntry
    
    private class PendingWrite(val startOffset: Long, var reversedBuffers: List[DataBuffer]) extends QueueEntry 
    
    private class PendingFlush() extends QueueEntry
    
    private[this] var queue = Queue[QueueEntry]()
    private[this] var qbytes: Int = 0
    private[this] var writeEndMap = Map[Long, PendingWrite]()
    
    def queuedBytes: Int = qbytes
    
    def enqueueTruncation(newFileEnd: Long): Future[Unit] = {
      val f = new PendingTruncation(newFileEnd)
      queue = queue.enqueue(f)
      f.completePromise.future
    }
    
    def enqueueWrite(offset: Long, buffers: List[DataBuffer]): Future[Unit] = {
      val nbytes = bufferedSize(buffers)
      qbytes += nbytes
      val endOffset = offset + nbytes
      val pw = writeEndMap.get(endOffset) match {
        case None => 
          val pw = new PendingWrite(offset, buffers.reverse)
          queue = queue.enqueue(pw)
          writeEndMap += (endOffset -> pw)
          pw
        case Some(pw) =>
          val origNbytes = bufferedSize(pw.reversedBuffers)
          val newEndOffset = pw.startOffset + origNbytes + nbytes
          pw.reversedBuffers = buffers.reverse ++ pw.reversedBuffers
          writeEndMap -= endOffset
          writeEndMap += (newEndOffset -> pw)
          pw
      }
      pw.completePromise.future
    }
    
    def enqueueFlush(): Future[Unit] = {
      val f = new PendingFlush()
      queue = queue.enqueue(f)
      f.completePromise.future
    }
    
    def dequeue(): Option[WriteOperation] = {
      if (queue.isEmpty) 
        None 
      else { 
        val t = queue.dequeue
        queue = t._2
        
        t._1 match {
          case tr: PendingTruncation => Some(new Truncate(tr.completePromise, tr.newFileEnd))
          
          case pw: PendingWrite =>
            val buffers = pw.reversedBuffers.reverse
            val nbytes = bufferedSize(buffers)
            qbytes -= nbytes
            val endOffset = pw.startOffset + nbytes
            writeEndMap -= endOffset
            Some(new Write(pw.completePromise, pw.startOffset, buffers))
            
          case f: PendingFlush => Some(new Flush(f.completePromise))
        }
      }
    }
  }
}

/** If the currently queued number of bytes is less than writeBufferSize, the write method will
 *  enqueue the write operation and the returned future will complete immediately. Otherwise the
 *  future will complete once the the full write operation has been successfully committed. Files
 *  opened in 'direct I/O' should set the buffer size to zero to ensure all writes complete before
 *  returning. 
 */
class SimpleFileHandle(
    val file: File,
    val writeBufferSize: Int) extends FileHandle {
  
  import SimpleFileHandle._
  
  val ifc = new IndexedFileContent(file.fs, file.inode)
  
  private[this] val writeQueue = new WriteQueue()
  private[this] var writeInProgress = false
  
  def read(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[Option[DataBuffer]] = ifc.read(file.inode, offset, nbytes)
  
  def truncate(offset: Long)(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    val fcomplete = writeQueue.enqueueTruncation(offset)
    beginNextWrite()
    fcomplete
  }
  
  def flush()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    val fcomplete = writeQueue.enqueueFlush()
    beginNextWrite()
    fcomplete
  }
  
  def write(offset: Long, buffers: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    val fcomplete = writeQueue.enqueueWrite(offset, buffers)
    val queueSize = writeQueue.queuedBytes
    
    beginNextWrite()
    
    if (queueSize < writeBufferSize)
      Future.unit
    else
      fcomplete
  }
  
  private def beginNextWrite()(implicit ec: ExecutionContext): Unit = synchronized {
    if (!writeInProgress) {
      writeQueue.dequeue().foreach { op => op match {
        case f: Flush => doFlush(f)
        case t: Truncate => doTruncation(t)
        case w: Write => doWrite(w)
      }}
    }
  }
  
  private def doFlush(flush: Flush)(implicit ec: ExecutionContext): Unit = {
    // Nothing to do. By virtue of getting here, all previously queued operations have completed
    flush.completePromise.success(())
  }
  
  private def doTruncation(tr: Truncate)(implicit ec: ExecutionContext): Unit = {
    writeInProgress = true
    
    def abort(t: Throwable) = synchronized {
      writeInProgress = false
      beginNextWrite() // probably hopeless but it'll prevent us from infinitely queueing data
      tr.completePromise.failure(t)
      throw new StopRetrying(t)
    }
    
    // Fail only if the inode has been deleted or a corrupted object is encountered
    def onAttemptFailure(t: Throwable): Future[Unit] = t match { 
      case t: CorruptedObject => abort(t)
      case _ => file.refresh().flatMap(_ => ifc.refresh()).recover { case t: InvalidObject => abort(t) }
    }
    
    val ftx = file.fs.system.transactUntilSuccessfulWithRecovery(onAttemptFailure _) { implicit tx =>
      
      ifc.truncate(file.inode, tr.newFileEnd) map { ws =>
        val inode = file.inode
        
        val base = inode.content + Inode.setMtime(Timespec.now) +
          (FileInode.FileSizeKey -> Value(FileInode.FileSizeKey, Varint.unsignedLongToArray(tr.newFileEnd), tx.timestamp))
          
        val newContent = ws.newRoot match {
          case None => base - FileInode.FileIndexRootKey
          case Some(p) => base + (FileInode.FileIndexRootKey -> Value(FileInode.FileIndexRootKey, p.toArray, tx.timestamp)) 
        }
        
        tx.overwrite(inode.pointer.pointer, inode.revision, Nil, KeyValueOperation.contentToOps(newContent))
        
        (tx.txRevision, tx.timestamp, newContent)
      }
    }
    
    ftx.foreach { t => 
      synchronized {
        //updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value], newRefcount: Option[ObjectRefcount]): Unit
        file.updateInode(t._1, t._2, t._3, None)
        writeInProgress = false
        beginNextWrite()
      }
      tr.completePromise.success(())
    }
  }
  
  private def doWrite(wop: Write)(implicit ec: ExecutionContext): Unit = {
    writeInProgress = true
    
    def abort(t: Throwable) = synchronized {
      writeInProgress = false
      beginNextWrite() // probably hopeless but it'll prevent us from infinitely queueing data
      wop.completePromise.failure(t)
      throw new StopRetrying(t)
    }
    
    // Fail only if the inode has been deleted or a corrupted object is encountered
    def onAttemptFailure(t: Throwable): Future[Unit] = t match { 
      case t: CorruptedObject => abort(t)
      case _ => file.refresh().flatMap(_ => ifc.refresh()).recover { case t: InvalidObject => abort(t) }
    }
    
    def writeSome(writeOffset: Long, writeBuffs: List[DataBuffer]): Unit = {
      if (writeBuffs.isEmpty) {
        synchronized {
          writeInProgress = false
          beginNextWrite()
        }
        wop.completePromise.success(())
      }
      else {
        val totalBytes = bufferedSize(writeBuffs)
    
        val ftx = file.fs.system.transactUntilSuccessfulWithRecovery(onAttemptFailure _) { implicit tx =>
          
          ifc.write(file.inode, writeOffset, writeBuffs).map { ws =>
            val inode = file.inode
            val nwritten = totalBytes - bufferedSize(ws.remainingData)
            
            val newSize = if (writeOffset + nwritten > inode.size) writeOffset + nwritten else inode.size
            
            val base = inode.content + Inode.setMtime(Timespec.now) + 
              (FileInode.FileSizeKey -> Value(FileInode.FileSizeKey, Varint.unsignedLongToArray(newSize), tx.timestamp))
              
            val newContent = ws.newRoot match {
              case None => base
              case Some(p) => base + (FileInode.FileIndexRootKey -> Value(FileInode.FileIndexRootKey, p.toArray, tx.timestamp)) 
            }
            
            tx.overwrite(inode.pointer.pointer, inode.revision, Nil, KeyValueOperation.contentToOps(newContent))
            
            val updatedInode = inode.update(tx.txRevision, tx.timestamp, newSize, ws.newRoot.map(_.toArray), inode.refcount)
            
            (ws.remainingOffset, ws.remainingData, tx.txRevision, tx.timestamp, newContent)
          }
        }
        
        ftx.foreach { t =>
          val (remainingOffset, remainingData, rev, ts, newContent) = t
          file.updateInode(rev, ts, newContent, None)
          writeSome(remainingOffset, remainingData)
        }
      }
    }
    
    writeSome(wop.startOffset, wop.buffers)
  }
  
}