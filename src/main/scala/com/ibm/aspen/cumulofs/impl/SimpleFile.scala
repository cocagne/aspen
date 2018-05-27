package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.cumulofs.FileInode
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.FilePointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.keyvalue.Value
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.base.ObjectAllocater
import scala.concurrent.Promise
import com.ibm.aspen.cumulofs.Inode
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.util.Varint
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.cumulofs.File
import com.ibm.aspen.cumulofs.Timespec
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.base.StopRetrying
import com.ibm.aspen.cumulofs.error.HolesNotSupported
import com.ibm.aspen.core.objects.ObjectRefcount

class SimpleFile(
    fs: FileSystem,
    cache: FileIndex.IndexNodeCache,
    segmentSize: Int,
    segmentAllocater: ObjectAllocater,
    protected var inode: FileInode) extends SimpleBaseFile(fs) with File {
  
  val pointer: FilePointer = inode.pointer
  
  val index = new FileIndex(fs, cache, inode)
  
  private[this] var queuedAppendOpsCount = 0
  private[this] var queuedWriteOpsCount = 0
  
  // If one or more append operations are queued, we'll add incoming append buffers here. When the append op is
  // full or the final outstanding append op is complete, it'll be queued for execution
  private[this] var tailAppendOp: Option[AppendOperation] = None
  
  // For batching contiguous writes, we'll keep track of the tail write operation (if any). If the next write begins
  // where the next ends and the tail has not yet been queued, we'll add it to the tail. When the write op is
  // full or the final outstanding write op is complete, it'll be queued for execution
  private[this] var tailWriteOp: Option[ContiguousWriteOperation] = None
  
  // Tracks the state of the end-of-file data object
  private[this] var dataTail: Option[Option[FileIndex.DataTail]] = None
  
  override protected def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value], newRefcount: Option[ObjectRefcount]): Unit = {
   inode = new FileInode(inode.pointer, newRevision, newRefcount.getOrElse(inode.refcount), newTimestamp, updatedState)
  }
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    dataTail = None
    fs.inodeLoader.load(inode.pointer).map { refreshedInode => synchronized {
      inode = refreshedInode
    }}
  }
  
  override def freeResources()(implicit ec: ExecutionContext): Future[Unit] = index.destroy()
  
  override def size: Long = synchronized { inode.size }
  
  def getDataTail()(implicit ec: ExecutionContext): Future[Option[FileIndex.DataTail]] = synchronized {
    dataTail match {
      case Some(odt) => Future.successful(odt)
      case None => index.getDataTail() map { odt => synchronized {
        dataTail = Some(odt)
        odt
      }}
    }
  }
  
  def debugRead()(implicit ec: ExecutionContext): Future[Array[Byte]] = synchronized {
    val arr = new Array[Byte](size.asInstanceOf[Int])
    val bb = ByteBuffer.wrap(arr)
    index.debugGetAllSegments() flatMap { segments =>
      def rfill(index: Int): Future[Unit] = if (index == segments.length) Future.successful(()) else {
        fs.system.readObject(segments(index).pointer) flatMap { dos =>
          bb.put(dos.data.asReadOnlyBuffer())
          rfill(index+1)
        }
      }
      rfill(0).map(_ => arr)
    }
  }

  private[this] def enqueueWriteOp(op: ContiguousWriteOperation)(implicit ec: ExecutionContext): Unit = {
    queuedWriteOpsCount += 1
    enqueueOp(op)
  }
  
  def write(offset: Long, data: DataBuffer)(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    val two = tailWriteOp match {
      case None =>
        val two = new ContiguousWriteOperation(offset, data)
        
        if (queuedWriteOpsCount >= 1)
          tailWriteOp = Some(two)
        else
          enqueueWriteOp(two)
          
        two
        
      case Some(two) =>
        if (offset == two.endOffset && two.addBuffer(data))
          two
        else {
          // Tail buffer is full. Enqueue for execution and start the next tail
          enqueueWriteOp(two)
          val newTwo = new ContiguousWriteOperation(offset, data)
          tailWriteOp = Some(newTwo)
          newTwo
        }
    }
    
    two.result
  }
  
  def write(offset: Long, data: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    if (data.isEmpty)
      return Future.successful(())
      
    def createOps(writeOffset: Long, ldata: List[DataBuffer], ops: List[ContiguousWriteOperation]): List[ContiguousWriteOperation] = {
      var nextOffset = writeOffset
      if (ldata.isEmpty)
        ops
      else {
        val two = new ContiguousWriteOperation(writeOffset, data.head)
        var l = data.tail
        while (!l.isEmpty && two.addBuffer(l.head)) {
          nextOffset += l.head.size
          l = l.tail
        }
        createOps(nextOffset, l, two :: ops)
      }
    }
    
    val rops = createOps(offset, data, Nil)
    val lastOp = rops.head
    val ops = rops.reverse
      
    tailWriteOp foreach { op =>
      tailWriteOp = None
      enqueueWriteOp(op)
    }
    
    ops.foreach(enqueueWriteOp)
    
    lastOp.result
  }
  
  // -- Append --
  
  private[this] def enqueueAppendOp(op: AppendOperation)(implicit ec: ExecutionContext): Unit = {
    queuedAppendOpsCount += 1
    enqueueOp(op)
  }
  
  def append(data: DataBuffer)(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    val tao = tailAppendOp match {
      case None =>
        val tao = new AppendOperation(data)
        
        if (queuedAppendOpsCount >= 1)
          tailAppendOp = Some(tao)
        else
          enqueueAppendOp(tao)
          
        tao
        
      case Some(tao) =>
        if (tao.addBuffer(data))
          tao
        else {
          // Tail buffer is full. Enqueue for execution and start the next tail
          enqueueAppendOp(tao)
          val newTao = new AppendOperation(data)
          tailAppendOp = Some(newTao)
          newTao
        }
    }
    
    tao.result
  }
  
  def append(data: List[DataBuffer])(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    if (data.isEmpty)
      return Future.successful(())
      
    def createOps(ldata: List[DataBuffer], ops: List[AppendOperation]): List[AppendOperation] = {
      if (ldata.isEmpty)
        ops
      else {
        val tao = new AppendOperation(data.head)
        var l = data.tail
        while (!l.isEmpty && tao.addBuffer(l.head))
          l = l.tail
        createOps(l, tao :: ops)
      }
    }
    
    val rops = createOps(data, Nil)
    val lastOp = rops.head
    val ops = rops.reverse
      
    tailAppendOp foreach { op =>
      tailAppendOp = None
      enqueueAppendOp(op)
    }
    
    ops.foreach(enqueueAppendOp)
    
    lastOp.result
  }
  
  abstract class AbstractWriteOperation(initialBuffer: DataBuffer) extends SimpleBaseFile.FileOperation {
    
    private[this] var rbuffers = List(initialBuffer)
    private[this] var rbufbytes = initialBuffer.size
    
    protected def numBufferedBytes: Int = rbufbytes
    
    def addBuffer(db: DataBuffer): Boolean = synchronized {
      if (rbufbytes + db.size / segmentSize > 5)
        false
      else {
        rbuffers = db :: rbuffers
        rbufbytes += db.size
        true
      }
    }
    
    def onOpComplete()(implicit ec: ExecutionContext): Unit
    
    /** Overwrites existing segments. Returns the remaining buffers that must go into newly allocated segments.
     *  And the number of bytes the current tail segment was extended by
     */
    def overwriteSegments(
        odt: Option[FileIndex.DataTail], 
        buffers: List[DataBuffer], 
        nbytes: Int)(implicit tx: Transaction, ec: ExecutionContext): Future[(List[DataBuffer], Int)]
    
    /** Allocates additional segments to be added to the file. Returns optional updated Root & Tail state as well as a Future
     *  to the index's internal state being successfully updated to reflect the transaction commit. This future must
     *  complete before the next operation begins. Note this method will only be called if dataBufs is non-empty.
     */
    def allocateAdditionalSegments(
        curInode: Inode,
        odt: Option[FileIndex.DataTail],
        dataBufs: List[DataBuffer],
        bufferdBytes: Int)(implicit tx: Transaction, ec: ExecutionContext): Future[(FileIndex.Root, FileIndex.DataTail, Future[Unit])] = {
      
      def ralloc(bufs: List[DataBuffer], remainingBytes: Int, arr: Array[Byte], bb: ByteBuffer, 
          allocations: List[Future[(DataObjectPointer, Int)]]): List[Future[(DataObjectPointer, Int)]]  = {
        if (bufs.isEmpty) {
          val falloc = segmentAllocater.allocateDataObject(curInode.pointer.pointer, curInode.revision, DataBuffer(arr)).map(p => (p, arr.length))
          
          falloc :: allocations
        }
        else if (bb.remaining == 0) {
          val nextArr = new Array[Byte]( if (remainingBytes < segmentSize) remainingBytes else segmentSize )
          val nextbb = ByteBuffer.wrap(nextArr)
          val nextAlloc = segmentAllocater.allocateDataObject(curInode.pointer.pointer, curInode.revision, DataBuffer(arr)).map(p => (p, arr.length))
          
          ralloc(bufs, remainingBytes, nextArr, nextbb, nextAlloc :: allocations)
        } 
        else if (bb.remaining >= bufs.head.size) {
          bb.put(bufs.head.asReadOnlyBuffer())
          ralloc(bufs.tail, remainingBytes - bufs.head.size, arr, bb, allocations)
        } else {
          val nput = bb.remaining
          bb.put(bufs.head.slice(0, nput).asReadOnlyBuffer())
          ralloc(bufs.head.slice(nput) :: bufs.tail, remainingBytes - nput, arr, bb, allocations)
        }
      }

      val initialArr = new Array[Byte]( if (bufferdBytes < segmentSize) bufferdBytes else segmentSize )
      val initialbb = ByteBuffer.wrap(initialArr)
      
      Future.sequence(ralloc(dataBufs, bufferdBytes, initialArr, initialbb, Nil)) flatMap { rallocs =>
        
        val allocs = rallocs.reverse
        
        val last = allocs.last
        
        val (newFileSize, firstAllocOffset) = odt match {
          case None => (bufferdBytes.asInstanceOf[Long], 0L)
          case Some(dt) => (dt.offset + segmentSize + bufferdBytes, dt.offset + segmentSize)
        }
       
        val newDt = FileIndex.DataTail(last._1, tx.txRevision, newFileSize - last._2, last._2)
        
        index.prepareAppend(curInode.pointer.pointer, curInode.revision, curInode.timestamp, firstAllocOffset, allocs) map { t =>
          (t._1, newDt, t._2)
        }
      }
    }
    
    /** Fills the byte buffer using the data in bufs. The returned list is the remaining data to be written. If the
     *  last list element does not exactly fit into the byte buffer the data buffer is sliced and the remaining content
     *  from it will be the first element in the returned list
     */
    protected def rfill(bb: ByteBuffer, bufs: List[DataBuffer]): List[DataBuffer] = {
      if (bufs.isEmpty)
        Nil
      else if (bb.remaining == 0)
        bufs
      else if (bb.remaining >= bufs.head.size) {
        bb.put(bufs.head.asReadOnlyBuffer())
        rfill(bb, bufs.tail)
      } else {
        val nput = bb.remaining
        bb.put(bufs.head.slice(0, nput).asReadOnlyBuffer())
        rfill(bb, bufs.head.slice(nput) :: bufs.tail)
      }
    }
    
    def attempt(curInode: Inode)(implicit tx: Transaction, ec: ExecutionContext): SimpleBaseFile.OpResult = synchronized {
      val pprep       = Promise[Unit]()
      val pcomplete   = Promise[Map[Key,Value]]()
      val buffers     = rbuffers.reverse
      val nbytes      = rbufbytes
      
      def allocSegments(
          odt: Option[FileIndex.DataTail],
          dataBufs: List[DataBuffer],
          numAppendBytes: Int): Future[Option[(FileIndex.Root, FileIndex.DataTail, Future[Unit])]] = if (dataBufs.isEmpty) 
        Future.successful(None)
      else {
        allocateAdditionalSegments(curInode, odt, dataBufs, numAppendBytes).map(Some(_))
      }
      
      val fprep = for {
        odt <- getDataTail()
        (remainingBufs, tailExtendedBytes) <- overwriteSegments(odt, buffers, nbytes)
        numAppendBytes = remainingBufs.foldLeft(0)( (sz, db) => sz + db.size )
        oupdate <- allocSegments(odt, remainingBufs, numAppendBytes)
      }
      yield {
        
        val (updatedContent, newDataTail, onewRoot, fstateUpdated) = oupdate match {
          case None =>
            val dt = odt.get
            
            val newDt = FileIndex.DataTail(dt.pointer, tx.txRevision, dt.offset, dt.size + tailExtendedBytes)
            
            val newFileSize = newDt.offset + newDt.size
            
            val szUpdate = (FileInode.FileSizeKey -> Value(FileInode.FileSizeKey, Varint.unsignedLongToArray(newFileSize), tx.timestamp))
            val mtimeUpdate = (Inode.MtimeKey -> Value(Inode.MtimeKey, Timespec.now.toArray, tx.timestamp))
            
            val newContent = curInode.content + mtimeUpdate + szUpdate 

            (newContent, newDt, None, Future.successful(()))
            
          case Some((newRoot, newDataTail, fstateUpdated)) =>
            val newFileSize = newDataTail.offset + newDataTail.size
            
            val newContent = curInode.content + 
            (FileInode.FileIndexRootKey -> Value(FileInode.FileIndexRootKey, newRoot.toArray(), tx.timestamp)) + 
            (FileInode.FileSizeKey -> Value(FileInode.FileSizeKey, Varint.unsignedLongToArray(newFileSize), tx.timestamp)) +
            (Inode.MtimeKey -> Value(Inode.MtimeKey, Timespec.now.toArray, tx.timestamp))
            
            (newContent, newDataTail, Some(newRoot.toArray), fstateUpdated)
        }
        
        val newFileSize = newDataTail.offset + newDataTail.size
        
        tx.overwrite(curInode.pointer.pointer, curInode.revision, Nil, KeyValueOperation.contentToOps(updatedContent))
        
        fstateUpdated onComplete {
          case Failure(cause) => pcomplete.failure(cause)
          case Success(_) => 
            synchronized {
              dataTail = Some(Some(newDataTail))
              inode = curInode.asInstanceOf[FileInode].update(tx.txRevision, tx.timestamp, newFileSize, onewRoot, curInode.refcount)
              
              onOpComplete()
              
            }
            
            pcomplete.success(updatedContent)
        }
      }
      
      fprep.failed.foreach(cause => pcomplete.failure(cause)) 
     
      SimpleBaseFile.OpResult(fprep, pcomplete.future)
    }
  }
  
  class AppendOperation(initialBuffer: DataBuffer) extends AbstractWriteOperation(initialBuffer) {
    
    def onOpComplete()(implicit ec: ExecutionContext): Unit = {
      queuedAppendOpsCount -= 1
      if (queuedAppendOpsCount == 0) {
        tailAppendOp.foreach { op =>
          tailAppendOp = None
          enqueueAppendOp(op)
        }
      }
    }
    
    def overwriteSegments(
        odt: Option[FileIndex.DataTail], 
        buffers: List[DataBuffer], 
        nbytes: Int)(implicit tx: Transaction, ec: ExecutionContext): Future[(List[DataBuffer], Int)] = {
      odt match {
        case None => Future.successful((buffers, 0))
        
        case Some(dt) => 
          val spaceLeft = segmentSize - dt.size
          
          if (spaceLeft == 0)
            Future.successful((buffers, 0))
          else {
            val arr = new Array[Byte]( if (spaceLeft >= nbytes) nbytes else spaceLeft )
            val bb = ByteBuffer.wrap(arr)
            
            val remainingBuffers = rfill(bb, buffers)
            
            tx.append(dt.pointer, dt.revision, DataBuffer(arr))
            
            Future.successful((remainingBuffers, arr.length))
          }
        }
    }
  }
  
  class ContiguousWriteOperation(offset: Long, initialBuffer: DataBuffer) extends AbstractWriteOperation(initialBuffer) {
    
    def endOffset: Long = synchronized { offset + numBufferedBytes }
    
    def onOpComplete()(implicit ec: ExecutionContext): Unit = {
      queuedWriteOpsCount -= 1
      if (queuedWriteOpsCount == 0) {
        tailWriteOp.foreach { op =>
          tailWriteOp = None
          enqueueWriteOp(op)
        }
      }
    }
    
    def getObjectsForRange(nbytes: Int)(implicit ec: ExecutionContext): Future[List[(Long, DataObjectState)]] = {
      index.getIndexNodeForOffset(offset + nbytes) flatMap { otail => otail match {
        case None => Future.successful(Nil)
        case Some(node) => 
          node.getSegmentsForRange(offset, nbytes).map(lst => lst.map(s => fs.system.readObject(s.pointer).map(dos => (s.offset, dos)))) flatMap { flf =>
            Future.sequence(flf)
          }
      }}
    }
    
    // Find tail segment of the range. If None or offset + segmentSize is less than offset + nbytes, we'll be appending
    def overwriteSegments(
        odt: Option[FileIndex.DataTail], 
        buffers: List[DataBuffer], 
        nbytes: Int)(implicit tx: Transaction, ec: ExecutionContext): Future[(List[DataBuffer], Int)] = {
      
      odt match {
        case None => if (offset != 0) throw StopRetrying( HolesNotSupported() )
        case Some(dt) => if (offset > dt.offset + dt.size) throw StopRetrying( HolesNotSupported() )
      }
      
      getObjectsForRange(nbytes) map { toOverwrite => 
        if (toOverwrite.isEmpty)
          (buffers, 0)
        else {
          def roverwrite(
              segmentOffset: Int, 
              existing: List[(Long, DataObjectState)], 
              remainingBuffers: List[DataBuffer], 
              extendedBytes: Int, 
              remainingBytes: Int): (List[DataBuffer], Int) = {
            
            if (remainingBuffers.isEmpty || existing.isEmpty)
              (remainingBuffers, extendedBytes)
            else {
              val dos = existing.head._2
              val writeEndOffset = segmentOffset + remainingBytes
              
              val (allocSize, extendingSize) = if (writeEndOffset >= segmentSize) 
                (segmentSize, 0) // extending size is only used if we don't allocate new segments so we can just put zero here 
              else if (writeEndOffset > dos.data.size)
                (writeEndOffset, writeEndOffset - dos.data.size)
              else
                (dos.data.size, 0)
              
              val arr = new Array[Byte]( allocSize )
              val bb = ByteBuffer.wrap(arr)
              
              bb.put(dos.data.asReadOnlyBuffer())
              
              bb.position(segmentOffset)
              
              val reducedBuffers = rfill(bb, remainingBuffers)
              
              tx.overwrite(dos.pointer, dos.revision, DataBuffer(arr))
              
              roverwrite(0, existing.tail, reducedBuffers, extendingSize, remainingBytes - (arr.length - segmentOffset))
            }
          }
          
          val initialSegmentOffset = (offset - toOverwrite.head._1).asInstanceOf[Int]
          roverwrite(initialSegmentOffset, toOverwrite, buffers, 0, nbytes)
        }
      }
    }
  }
}