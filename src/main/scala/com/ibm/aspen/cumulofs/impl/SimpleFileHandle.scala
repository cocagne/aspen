package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.cumulofs.{File, FileHandle}

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

object SimpleFileHandle {

  private class PendingWrite(val offset: Long,
                             var nbytes: Int,
                             var reversedBuffers: List[DataBuffer]) {

    val completePromise: Promise[Unit] = Promise()

    def complete: Future[Unit] = completePromise.future

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

  private[this] var writeQueue: Queue[PendingWrite] = Queue()
  private[this] var ocurrentWrite: Option[PendingWrite] = None
  private[this] var nbuffered: Int = 0

  def read(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[Option[DataBuffer]] = {
    file.read(offset, nbytes)
  }

  def truncate(offset: Long)(implicit ec: ExecutionContext): Future[Future[Unit]] = file.truncate(offset)

  def flush()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    ocurrentWrite match {
      case None => Future.unit
      case Some(pw) => if (writeQueue.isEmpty) pw.complete else writeQueue.last.complete
    }
  }

  def write(offset: Long, buffers: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    val wsize = buffers.foldLeft(0)((sz, db) => sz + db.size)
    val rbuffers = buffers.reverse

    nbuffered += wsize

    val pw = writeQueue.find(_.endOffset == offset) match {
      case None =>
        val p = new PendingWrite(offset, wsize, rbuffers)
        writeQueue = writeQueue.enqueue(p)
        p

      case Some(p) =>
        p.nbytes += wsize
        p.reversedBuffers = rbuffers ++ p.reversedBuffers
        p
    }

    beginNextWrite()

    if (nbuffered < writeBufferSize)
      Future.unit
    else
      pw.complete
  }

  private[this] def beginNextWrite()(implicit ec: ExecutionContext): Unit = synchronized {
    if (ocurrentWrite.isEmpty) {

      writeQueue.dequeueOption.foreach { t =>
        val pw = t._1
        writeQueue = t._2

        ocurrentWrite = Some(pw)

        def writeSome(offset: Long, buffers: List[DataBuffer]): Unit = file.write(offset, buffers).foreach { t =>
          val (remainingOffset, remainingBuffers) = t

          if (remainingBuffers.isEmpty) {
            synchronized {
              nbuffered -= pw.nbytes
              ocurrentWrite = None
              pw.completePromise.success(())
              beginNextWrite()
            }
          }
          else
            writeSome(remainingOffset, remainingBuffers)
        }

        writeSome(pw.offset, pw.reversedBuffers.reverse)
      }
    }
  }
  
}