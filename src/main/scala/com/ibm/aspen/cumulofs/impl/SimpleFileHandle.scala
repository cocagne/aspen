package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.cumulofs.{File, FileHandle}
import org.apache.logging.log4j.scala.Logging

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

object SimpleFileHandle {

  private class PendingWrite(val writeNumber: Int,
                             val offset: Long,
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
    val writeBufferSize: Int) extends FileHandle with Logging {
  
  import SimpleFileHandle._

  private[this] var writeQueue: Queue[PendingWrite] = Queue()
  private[this] var ocurrentWrite: Option[PendingWrite] = None
  private[this] var nbuffered: Int = 0
  private[this] var writeCount: Int = 0
  private[this] var readCache: Option[(Long, DataBuffer)] = None

  // Always try to read a megabyte at a time and serve reads from the cached content if possible
  def read(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[Option[DataBuffer]] = synchronized {
    val odata = readCache match {
      case None => None
      case Some((doffset, db)) =>
        if (offset >= doffset && offset + nbytes <= doffset + db.size)
          Some(db.slice((offset-doffset).asInstanceOf[Int], nbytes))
        else
          None
    }

    odata match {
      case Some(db) => Future.successful(Some(db))
      case None =>
        val rsize = if (nbytes > 1024*1024) nbytes else 1024*1024
        file.read(offset, rsize).map {
          case None => None

          case Some(data) =>
            synchronized {
              readCache = Some((offset, data))
              Some(data.slice(0, nbytes))
            }
        }
    }
  }

  def truncate(offset: Long)(implicit ec: ExecutionContext): Future[Future[Unit]] = synchronized {
    readCache = None
    file.truncate(offset)
  }

  def flush()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    ocurrentWrite match {
      case None => Future.unit
      case Some(pw) => if (writeQueue.isEmpty) pw.complete else writeQueue.last.complete
    }
  }

  def write(offset: Long, buffers: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit] = synchronized {

    // TODO - Track down source of corruption on DataBuffers being passed into this method. While queued, the head
    //        buffers are occasionally being overwritten
    val copies = buffers.map(_.copy())

    val wsize = copies.foldLeft(0)((sz, db) => sz + db.size)
    val rbuffers = copies.reverse

    nbuffered += wsize

    val pw = writeQueue.find(_.endOffset == offset) match {
      case None =>
        writeCount += 1
        logger.info(s"Queuing new write number $writeCount at offset $offset, end offset ${offset + wsize}. CRCs ${copies.map(_.hashString)}")
        val p = new PendingWrite(writeCount, offset, wsize, rbuffers)
        writeQueue = writeQueue.enqueue(p)
        p

      case Some(p) =>
        logger.info(s"Buffering write in pending write number ${p.writeNumber} with end offset $offset, new end offset ${offset + wsize} CRCs ${copies.map(_.hashString)}")
        p.nbytes += wsize
        p.reversedBuffers = rbuffers ++ p.reversedBuffers
        p
    }

    beginNextWrite()

    if (nbuffered < writeBufferSize) {
      logger.info(s"Returning immediately. $nbuffered < $writeBufferSize")
      Future.unit
    } else {
      logger.info(s"Returning future to write completion ! $nbuffered < $writeBufferSize")
      pw.complete
    }
  }

  private[this] def beginNextWrite()(implicit ec: ExecutionContext): Unit = synchronized {
    if (ocurrentWrite.isEmpty) {

      writeQueue.dequeueOption.foreach { t =>
        val pw = t._1
        writeQueue = t._2

        ocurrentWrite = Some(pw)

        def writeSome(offset: Long, buffers: List[DataBuffer]): Unit = {
          logger.info(s"  write ${pw.writeNumber}: writeSome(offset=$offset, bufferCount=${buffers.length}, nbytes=${buffers.foldLeft(0)((sz,b) => sz+b.remaining())})")
          val sb = new StringBuilder
          buffers.foldLeft(offset) { (off, db) =>
            sb.append(s"    offset $off crc: ${db.hashString}\n")
            off + db.size
          }
          logger.info(sb.toString)

          file.write(offset, buffers).foreach { t =>
            val (remainingOffset, remainingBuffers) = t

            if (remainingBuffers.isEmpty) {
              synchronized {
                readCache = None
                nbuffered -= pw.nbytes
                ocurrentWrite = None
                logger.info(s"Completed write ${pw.writeNumber} at offset ${pw.offset}, endOffset ${pw.endOffset}")
                pw.completePromise.success(())
                beginNextWrite()
              }
            }
            else
              writeSome(remainingOffset, remainingBuffers)
          }
        }

        logger.info(s"Beginning write ${pw.writeNumber} at offset ${pw.offset}, endOffset ${pw.endOffset}, numBuffers: ${pw.reversedBuffers.size}")
        writeSome(pw.offset, pw.reversedBuffers.reverse)
      }
    }
  }
  
}