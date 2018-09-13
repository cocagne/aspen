package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.cumulofs.{File, FileHandle}

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

object SimpleFileHandle {

  class PendingWrite(val offset: Long,
                     val nbytes: Int,
                     val reversedBuffers: List[DataBuffer],
                     opromise: Option[Promise[Unit]]) {

    val completePromise: Promise[Unit] = opromise.getOrElse(Promise())

    def endOffset: Long = offset + nbytes
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

  private[this] var bufferingWrite: Option[PendingWrite] = None
  private[this] var writeQueue: Queue[PendingWrite] = Queue()
  private[this] var writeOutstanding = false
  
  def read(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[Option[DataBuffer]] = {
    file.read(offset, nbytes)
  }
  
  def truncate(offset: Long)(implicit ec: ExecutionContext): Future[Unit] = file.truncate(offset)
  
  def flush()(implicit ec: ExecutionContext): Future[Unit] = file.flush()

  def write(offset: Long, buffers: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    val wsize = buffers.foldLeft(0)((sz, db) => sz + db.size)
    val rbuffers = buffers.reverse

    val npw = if (bufferingWrite.exists(_.endOffset == offset)) {
      val pw = bufferingWrite.get
      new PendingWrite(pw.offset, pw.nbytes + wsize, rbuffers ++ pw.reversedBuffers, Some(pw.completePromise))
    } else {
      bufferingWrite.foreach(pw => writeQueue = writeQueue.enqueue(pw))
      new PendingWrite(offset, wsize, rbuffers, None)
    }

    bufferingWrite = Some(npw)

    beginNextWrite()

    npw.completePromise.future
  }

  private[this] def beginNextWrite()(implicit ec: ExecutionContext): Unit = synchronized {
    if (!writeOutstanding) {

      val nextPw = if (writeQueue.nonEmpty) {
        val (pw, newQueue) = writeQueue.dequeue
        writeQueue = newQueue
        Some(pw)
      } else if (bufferingWrite.nonEmpty) {
        val t = bufferingWrite
        bufferingWrite = None
        t
      } else None

      nextPw.foreach { pw =>

        writeOutstanding = true

        def writeSome(offset: Long, buffers: List[DataBuffer]): Unit = {
          file.write(offset, buffers).foreach { t =>
            val (remainingOffset, remainingBuffers) = t

            if (remainingBuffers.isEmpty) {
              pw.completePromise.success(())
              synchronized {
                writeOutstanding = false
                beginNextWrite()
              }
            } else
              writeSome(remainingOffset, remainingBuffers)
          }
        }

        writeSome(pw.offset, pw.reversedBuffers.reverse)
      }
    }
  }
  
}