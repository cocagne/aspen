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

class SimpleFile(
    fs: FileSystem,
    cache: FileIndex.IndexNodeCache,
    segmentSize: Int,
    segmentAllocater: ObjectAllocater,
    protected var inode: FileInode) extends SimpleBaseFile(fs) with File {
  
  val pointer: FilePointer = inode.pointer
  
  val index = new FileIndex(fs, cache, inode)
  
  // Queue of append operations. We use a mutable queue here to prevent holding on to
  // DataBuffer references loger than necessary 
  private[this] val appendOpBacklog = new scala.collection.mutable.Queue[AppendOperation]
  
  // True if an AppendOperation has been placed into the work queue
  private[this] var appendIsQueued = false
  
  // If an append operation is queued, we'll add incoming append buffers here. When the append op is
  // full, it'll be enqueued into the appendOpBacklog
  private[this] var tailAppendOp: Option[AppendOperation] = None
  
  // Tracks the state of the end-of-file data object
  private[this] var dataTail: Option[Option[FileIndex.DataTail]] = None
  
  override protected def updateInode(newRevision: ObjectRevision, newTimestamp: HLCTimestamp, updatedState: Map[Key,Value]): Unit = {
   inode = new FileInode(inode.pointer, newRevision, inode.refcount, newTimestamp, updatedState)
  }
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    dataTail = None
    fs.inodeLoader.load(inode.pointer).map { refreshedInode => synchronized {
      inode = refreshedInode
    }}
  }
  
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
  /* PendingWrite(val startOffset, var endOffset, buffers: List[DataBuffer])
   * 
   * SimpleFile keeps map of Map[EndOffset, PendingWrite] to allow adjacent writes to be buffered into the same pending write.
   * 
   * When write begins, it removes itself from the pendingWrites map to prevent additional items from being added
   * if not all data can be written in the same Tx, add a new pendingWrite to SimpleFile.
   * 
   * Only 1 pendingWrite and 1 append is allowed in the OpQueue at a time. Upon successful completion, enqueue a 
   * new op for the next item in the pending list
   * */
  def write(offset: Long, data: DataBuffer): Future[Unit] = ???
  
  def append(data: DataBuffer)(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    val tao = tailAppendOp match {
      case None =>
        val tao = new AppendOperation(data)
        
        if (appendIsQueued)
          tailAppendOp = Some(tao)
        else
          enqueueOp(tao)
          
        tao
      case Some(tao) =>
        if (tao.addBuffer(data))
          tao
        else {
          appendOpBacklog.enqueue(tao)
          val newTao = new AppendOperation(data)
          tailAppendOp = Some(newTao)
          newTao
        }
    }
    
    tao.result
  }
  
  
  
  class AppendOperation(initialBuffer: DataBuffer) extends SimpleBaseFile.FileOperation {
    
    private[this] var rbuffers = List(initialBuffer)
    private[this] var rbufbytes = initialBuffer.size
    
    def addBuffer(db: DataBuffer): Boolean = synchronized {
      if (rbufbytes + db.size / segmentSize > 5)
        false
      else {
        rbuffers = db :: rbuffers
        rbufbytes += rbufbytes + db.size
        true
      }
    }
    
    def attempt(curInode: Inode)(implicit tx: Transaction, ec: ExecutionContext): SimpleBaseFile.OpResult = synchronized {
      val pprep       = Promise[Unit]()
      val pcomplete   = Promise[Map[Key,Value]]()
      val nbytes      = rbufbytes
      
      def fillTailNode(odt: Option[FileIndex.DataTail]): (List[DataBuffer], Int) = odt match {
        case None => (rbuffers.reverse, nbytes)
        case Some(dt) => 
          val spaceLeft = segmentSize - dt.size
          val buffers = rbuffers.reverse
          
          if (spaceLeft == 0)
            return (buffers, nbytes)
          else {
            val arr = new Array[Byte]( if (spaceLeft >= nbytes) nbytes else spaceLeft )
            val bb = ByteBuffer.wrap(arr)
            
            def rfill(bufs: List[DataBuffer], remainingBytes: Int): (List[DataBuffer], Int) = {
              if (bufs.isEmpty)
                (Nil, remainingBytes)
              else if (bb.remaining == 0)
                (bufs, remainingBytes)
              else if (bb.remaining >= bufs.head.size) {
                bb.put(bufs.head.asReadOnlyBuffer())
                rfill(bufs.tail, remainingBytes - bufs.head.size)
              } else {
                val nput = bb.remaining
                bb.put(bufs.head.slice(0, nput).asReadOnlyBuffer())
                rfill(bufs.head.slice(nput) :: bufs.tail, remainingBytes - nput)
              }
            }
            
            val (remainingBuffers, remainingBytes) = rfill(buffers, nbytes)
            
            tx.append(dt.pointer, dt.revision, DataBuffer(arr))
            
            (remainingBuffers, remainingBytes)
          }
      }
      
      // Returned future must complete before next append operation may begin
      def allocateAdditionalSegments(
          odt: Option[FileIndex.DataTail],
          dataBufs: List[DataBuffer],
          bufferdBytes: Int): Future[Option[(FileIndex.Root, FileIndex.DataTail, Future[Unit])]] = if (dataBufs.isEmpty) 
        Future.successful(None)
      else {
        
        def ralloc(bufs: List[DataBuffer], remainingBytes: Int, arr: Array[Byte], bb: ByteBuffer, 
            allocations: List[Future[(DataObjectPointer, Int)]]): List[Future[(DataObjectPointer, Int)]]  = {
          if (bufs.isEmpty) {
            println(s"ALLOC .isEmpty")
            val falloc = segmentAllocater.allocateDataObject(curInode.pointer.pointer, curInode.revision, DataBuffer(arr)).map(p => (p, arr.length))
            falloc.onComplete {
              case Success(_) => println("  ALLOC.isEmpty success")
              case Failure(err) => println(s"  ALLOC.isEmpty failed $err")
            }
            falloc :: allocations
            
          }
          else if (bb.remaining == 0) {
            val nextArr = new Array[Byte]( if (remainingBytes < segmentSize) remainingBytes else segmentSize )
            val nextbb = ByteBuffer.wrap(nextArr)
            println(s"ALLOC bb.remaining == 0")
            val nextAlloc = segmentAllocater.allocateDataObject(curInode.pointer.pointer, curInode.revision, DataBuffer(arr)).map(p => (p, arr.length))
            nextAlloc .onComplete {
              case Success(_) => println("  ALLOC.bb.remaining == 0 success")
              case Failure(err) => println(s"  ALLOC.bb.remaining == 0 failed $err")
            }
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
          println("ALLOCS Complete")
          val allocs = rallocs.reverse
          
          val last = allocs.last
          
          val (newFileSize, firstAllocOffset) = odt match {
            case None => (nbytes.asInstanceOf[Long], 0L)
            case Some(dt) => (dt.offset + dt.size + nbytes, dt.offset + segmentSize)
          }
          
          val newDt = FileIndex.DataTail(last._1, tx.txRevision, newFileSize - last._2, last._2)
          
          index.prepareAppend(curInode.pointer.pointer, curInode.revision, curInode.timestamp, firstAllocOffset, allocs) map { t =>
            Some((t._1, newDt, t._2))
          }
        }
      }
      
      val fprep = for {
        odt <- getDataTail()
        (bufs, remainingBytes) = fillTailNode(odt)
        oupdate <- allocateAdditionalSegments(odt, bufs, remainingBytes)
      }
      yield {
        
        val newFileSize = odt match {
          case None => nbytes 
          case Some(dt) => dt.offset + dt.size + nbytes
        }
        
        val sz = (FileInode.FileSizeKey -> Value(FileInode.FileSizeKey, Varint.unsignedLongToArray(newFileSize), tx.timestamp))
        
        val (updatedContent, newDataTail, onewRoot, fstateUpdated) = oupdate match {
          case None =>
            val dt = odt.get
            val newDt = FileIndex.DataTail(dt.pointer, tx.txRevision, dt.offset, dt.size + nbytes)
            (curInode.content + sz, newDt, None, Future.successful(()))
            
          case Some((newRoot, newDataTail, fstateUpdated)) => 
            val newContent = curInode.content + sz + (FileInode.FileIndexRootKey -> Value(FileInode.FileIndexRootKey, newRoot.toArray(), tx.timestamp))
            
            (newContent, newDataTail, Some(newRoot.toArray), fstateUpdated)
        }
        
        tx.overwrite(curInode.pointer.pointer, curInode.revision, Nil, KeyValueOperation.contentToOps(updatedContent))
        
        fstateUpdated onComplete {
          case Failure(cause) => pcomplete.failure(cause)
          case Success(_) => synchronized {
            dataTail = Some(Some(newDataTail))
            inode = curInode.asInstanceOf[FileInode].update(tx.txRevision, tx.timestamp, newFileSize, onewRoot, curInode.refcount)
            pcomplete.success(updatedContent)
          }
        }
      }
      
      fprep.failed.foreach(cause => pcomplete.failure(cause)) 
     
      SimpleBaseFile.OpResult(fprep, pcomplete.future)
    }
  }
}