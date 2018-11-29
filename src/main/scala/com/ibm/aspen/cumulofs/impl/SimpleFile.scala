package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.{DataObjectPointer, ObjectRevision}
import com.ibm.aspen.cumulofs._
import com.ibm.aspen.cumulofs.impl.IndexedFileContent.WriteStatus
import com.ibm.aspen.cumulofs.impl.SimpleBaseFile.FileOperation

import scala.concurrent.{ExecutionContext, Future, Promise}

class SimpleFile(override val pointer: FilePointer,
                 cachedInodeRevision: ObjectRevision,
                 initialInode: FileInode,
                 fs: FileSystem,
                 osegmentSize: Option[Int]=None,
                 otierNodeSize: Option[Int]=None) extends SimpleBaseFile(pointer, cachedInodeRevision, initialInode, fs) with File {

  import SimpleFile._

  private var oifc: Option[IndexedFileContent] = None

  private def content: IndexedFileContent = synchronized {
    if (oifc.isEmpty)
      oifc = Some(new IndexedFileContent(this, osegmentSize, otierNodeSize))
    oifc.get
  }

  override protected def setCachedInode(newInode: Inode, newRevision:ObjectRevision): Unit = synchronized {
    super.setCachedInode(newInode, newRevision)
    if (inode.ocontents.isEmpty)
      oifc = None
  }

  override def inode: FileInode = super.inode.asInstanceOf[FileInode]

  override def inodeState: (FileInode, ObjectRevision) = {
    val t = super.inodeState
    (t._1.asInstanceOf[FileInode], t._2)
  }

  override def freeResources()(implicit ec: ExecutionContext): Future[Unit] = {
    new SimpleFileHandle(this, 0).truncate(0).map(_ => ())
  }

  def debugReadFully()(implicit ec: ExecutionContext): Future[Array[Byte]] = content.debugReadFully()

  def read(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[Option[DataBuffer]] = {
    content.read(offset, nbytes)
  }

  def write(offset: Long,
            buffers: List[DataBuffer])(implicit ec: ExecutionContext): Future[(Long, List[DataBuffer])] = {
    val op = Write(this, offset, buffers)
    enqueueOp(op)
    op.writePromise.future
  }

  def truncate(offset: Long)(implicit ec: ExecutionContext): Future[Future[Unit]] = {
    val op = Truncate(this, offset)
    enqueueOp(op).map(_ => op.deleteComplete)
  }
}

object SimpleFile {
  case class Truncate(file: SimpleFile, offset: Long) extends FileOperation {

    private val p = Promise[Unit]()

    def deleteComplete: Future[Unit] = p.future

    def prepareTransaction(pointer: DataObjectPointer,
                           revision: ObjectRevision,
                           inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): Future[Inode] = synchronized {
      file.content.truncate(offset).map { t =>
        val (ws, fdeleteComplete) = t

        fdeleteComplete.foreach(_ => p.success(()))

        val updatedInode = inode.asInstanceOf[FileInode].updateContent(offset, Timespec.now, ws.newRoot)
        tx.overwrite(pointer, revision, updatedInode.toDataBuffer)
        updatedInode
      }
    }

  }

  case class Write(file: SimpleFile, offset: Long, buffers: List[DataBuffer]) extends FileOperation {
    val writePromise: Promise[(Long, List[DataBuffer])] = Promise()

    def prepareTransaction(pointer: DataObjectPointer,
                           revision: ObjectRevision,
                           inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): Future[Inode] = synchronized {

      val finode = inode.asInstanceOf[FileInode]

      val totalSize = buffers.foldLeft(0)((sz, db) => sz + db.size)

      file.content.write(offset, buffers).map { ws =>

        val nwritten = totalSize - ws.remainingData.foldLeft(0)((sz, db) => sz + db.size)

        val endOffset = offset + nwritten

        val newSize = if (endOffset > finode.size) endOffset else finode.size

        val updatedInode = inode.asInstanceOf[FileInode].updateContent(newSize, Timespec.now, ws.newRoot)

        tx.overwrite(pointer, revision, updatedInode.toDataBuffer)

        tx.result.foreach { _ =>
          writePromise.success((ws.remainingOffset, ws.remainingData))
        }

        updatedInode
      }
    }
  }
}