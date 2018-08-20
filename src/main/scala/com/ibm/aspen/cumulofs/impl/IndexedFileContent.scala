package com.ibm.aspen.cumulofs.impl

import scala.concurrent.Future
import com.ibm.aspen.core.objects.DataObjectPointer
import java.nio.ByteBuffer
import com.ibm.aspen.cumulofs.FileSystem
import java.util.UUID
import com.ibm.aspen.core.objects.DataObjectState
import com.github.blemale.scaffeine.Scaffeine
import com.github.blemale.scaffeine.Cache
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.util.Varint
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.cumulofs.FileInode
import com.ibm.aspen.base.task.SteppedDurableTask
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.base.task.DurableTaskType
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.task.DurableTaskPointer
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.base.task.DurableTask
import com.ibm.aspen.core.read.InvalidObject

class IndexedFileContent(val fs: FileSystem, inode: FileInode, osegmentSize: Option[Int]=None, otierNodeSize: Option[Int]=None) {
  import IndexedFileContent._
  
  private[this] var otail: Option[Tail] = None
  
  private val inodePointer = inode.pointer
  
  def refresh()(implicit ec: ExecutionContext): Future[Unit] = refreshRoot.map(_=>())
  
  private def refreshRoot()(implicit ec: ExecutionContext): Future[IndexNode] = {
    
    dropCache()
    
    fs.inodeLoader.load(inodePointer) flatMap { inode => 
      inode.fileIndexRoot() match {
        case Some(arr) => load(DataObjectPointer(arr))
          
        case None => throw new FatalIndexError("Root Index Pointer Deleted")
      }
    }
  }
  
  private def getOrAllocateRoot(inode: FileInode)(implicit tx: Transaction, ec: ExecutionContext): Future[IndexNode] = {
    inode.fileIndexRoot match {
      case None =>
        dropCache()
        
        val content = IndexNode.getEncodedNodeContent()
        
        allocateIndexNode(inode.pointer.pointer, inode.revision, tier=0, content=content).map { newPointer =>
          //println(s"New Root Node: ${newPointer.uuid}")
          IndexNode(newPointer, ObjectRevision(tx.uuid), content, this)
        }
        
      case Some(inodeRootArr) => load(DataObjectPointer(inodeRootArr))
    }
  }
  
  private val segmentSize = osegmentSize.getOrElse(fs.defaultSegmentSize)
  
  private def tierNodeSize(tier: Int): Int = otierNodeSize.getOrElse(fs.getDataTableNodeSize(tier))
  
  private def read(pointer: DataObjectPointer): Future[DataObjectState] = fs.system.readObject(pointer)
  
  private val cache: Cache[UUID, IndexNode] = Scaffeine().maximumSize(50).build[UUID, IndexNode]()
  
  private def dropCache(): Unit = synchronized {
    otail = None
    cache.invalidateAll()
  }
  
  private def invalidateCachedNodes(l: List[IndexNode]): Unit = l.foreach(n => cache.invalidate(n.uuid))
  
  private def load(nodePointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[IndexNode] = {
    cache.getIfPresent(nodePointer.uuid) match {
      case Some(n) => Future.successful(n)
      case None => fs.system.readObject(nodePointer).flatMap { dos =>
        val n = IndexNode(dos.pointer, dos.revision, dos.data, this)
        //println(s"Loading Index Node ${n.uuid}. Tier ${n.tier} Entries: ${n.entries.toList.map(e => (e.offset -> e.pointer.uuid))}")
        cache.put(n.uuid, n)
        Future.successful(n)
      }
    }
  }
  
  private def allocateIndexNode(
      allocObj: ObjectPointer, 
      allocRev: ObjectRevision, 
      tier: Int, 
      content: DataBuffer)(implicit tx: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    fs.getDataTableNodeAllocater(tier).flatMap { allocater =>
      allocater.allocateDataObject(allocObj, allocRev, content, None)
    }
  }
  
  private def allocateDataSegment(
      allocObj: ObjectPointer, 
      allocRev: ObjectRevision, 
      content: DataBuffer)(implicit tx: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    fs.defaultSegmentAllocater().flatMap { allocater =>
      allocater.allocateDataObject(allocObj, allocRev, content, None)
    }
  }
  
  def read(inode: FileInode, offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[Option[DataBuffer]] = {
    inode.fileIndexRoot() match {
      case None => Future.successful(None)
      case Some(arr) =>
        
        val bb = ByteBuffer.allocate(nbytes)
        //println(s"Loading index")
        load(DataObjectPointer(arr)).flatMap { node =>
          //println(s"Index Node Loaded ${node.uuid}. offset: $offset. nbytes $nbytes Entries: ${node.entries.toList.map(d => (d.offset -> d.pointer.uuid))}")
          node.getIndexEntriesForRange(offset, nbytes).flatMap { t =>
            val (_, segments) = t
            //println(s"Entry for range: ${segments.map(x => (x._1.offset -> x._1.pointer.uuid))}")
            val fbufs = Future.sequence(segments.map { t => 
              val (d, _) = t
              read(d.pointer).map( dos => (d.offset, dos.data) )
            })
            
            fbufs.map { ebufs =>
              ebufs.foreach { t => 
                val (doffset, rawdb) = t
                
                val db = if (doffset < offset) rawdb.slice((offset - doffset).asInstanceOf[Int]) else rawdb
                
                if (doffset > offset)
                  bb.position((doffset - offset).asInstanceOf[Int])
                
                bb.put(if (bb.position + db.size > bb.limit) db.slice(0, bb.limit - bb.position) else db)
              }
              
              bb.position(0)
              Some(DataBuffer(bb))
            }
          }
        }
    }
  }
  
  def debugReadFully(inode: FileInode)(implicit ec: ExecutionContext): Future[Array[Byte]] = read(inode, 0, inode.size.asInstanceOf[Int]).map { odb => odb match {
    case None => new Array[Byte](0)
    case Some(db) => db.getByteArray()
  }}
  
  private def recursiveAlloc(
      segmentOffset: Long, 
      remaining: List[DataBuffer], 
      allocated: List[Future[(DownPointer, Int)]])(implicit tx: Transaction, ec: ExecutionContext): List[Future[(DownPointer, Int)]] = {
    //println(s"Recursive alloc. Seg size ${segmentSize} nbytes remaining ${remaining.foldLeft(0)((sz, db) => sz + db.size)}")
    if (remaining.isEmpty)
      allocated
    else {
      val rbytes = remaining.foldLeft(0)((sz, db) => sz + db.size)
      val arr = new Array[Byte](if (rbytes <= segmentSize) rbytes else segmentSize)
      val bb = ByteBuffer.wrap(arr)
      val leftover = DataBuffer.fill(bb, remaining)
      val falloc = allocateDataSegment(inode.pointer.pointer, inode.revision, DataBuffer(arr)).map { newPointer =>
        //println(s"Allocated data segment: ${newPointer.uuid}")
        (DownPointer(segmentOffset, newPointer), arr.length)
      }
      recursiveAlloc(segmentOffset + arr.length, leftover, falloc :: allocated)
    }
  }
  
  /** Returns remaining buffers and the offset at which they begin */
  private def updateSegment(segmentOffset: Long, dos: DataObjectState, 
      offset: Long, buffers: List[DataBuffer])(implicit tx: Transaction, ec: ExecutionContext): (List[DataBuffer], Long) = {
    
    val nbytes = buffers.foldLeft(0)((sz, db) => sz + db.size)
    val writeEnd = offset + nbytes
    val segmentEnd = segmentOffset + segmentSize
    val offsetInSegment = (offset - segmentOffset).asInstanceOf[Int]
    
    if (offset >= segmentEnd) {
      (buffers, offset)
    }
    else if (offsetInSegment >= dos.data.size) {
      val appendBuffers = if (offsetInSegment > dos.data.size) DataBuffer.zeroed(offsetInSegment - dos.data.size) :: buffers else buffers
      val maxAppendSize = segmentSize - dos.data.size
      val (appendBuff, remaining) = DataBuffer.compact(maxAppendSize, appendBuffers)
      tx.append(dos.pointer, dos.revision, appendBuff)
      (remaining, segmentOffset + dos.data.size + appendBuff.size)
    } 
    else {
      
      val objectSize = if (writeEnd >= segmentEnd) 
        segmentSize
      else if (writeEnd > segmentOffset + dos.data.size)
        (writeEnd - segmentOffset).asInstanceOf[Int]
      else
        dos.data.size
        
      val bb = ByteBuffer.allocate(objectSize)
      
      if (offset != segmentOffset)
        bb.put(dos.data.slice(0, (offset - segmentOffset).asInstanceOf[Int]))
      
      val (overwriteBuff, remaining) = DataBuffer.compact(bb.remaining(), buffers)
      bb.put(overwriteBuff)
      
      if (bb.remaining() != 0)
        bb.put(dos.data.slice(dos.data.size - bb.remaining()))
      
      bb.position(0)
      tx.overwrite(dos.pointer, dos.revision, DataBuffer(bb))
      
      (remaining, segmentOffset + objectSize)
    }
  }
  
  def truncate(inode: FileInode, endOffset: Long)(implicit tx: Transaction, ec: ExecutionContext): Future[WriteStatus] = {
    if (endOffset > inode.size) {
     write(inode, endOffset - 1, List(DataBuffer(Array[Byte](0)))) 
    }
    else if (inode.size == endOffset) {
      Future.successful(WriteStatus(inode.fileIndexRoot.map(DataObjectPointer(_)), 0, Nil, Future.unit))
    } else {
      def truncateOrDelete(root: IndexNode): Future[Option[DataObjectPointer]] = {
        if (endOffset == 0)
          prepareIndexDeletionTask(fs, root.pointer).map(_ => None)
        else
          root.truncate(endOffset).map(_ => Some(root.pointer))
      }
      for {
        root <- getOrAllocateRoot(inode)
        optr <- truncateOrDelete(root)
      } yield {
        val fcomplete = tx.result.map { _ =>
          dropCache()
        }
        WriteStatus(optr, 0, Nil, fcomplete)
      }
    }
  }
  
  /** Specialized write method for adding to the end of a file */
  private def writeTail(
    inode: FileInode, 
    offset: Long, 
    buffers: List[DataBuffer])(implicit tx: Transaction, ec: ExecutionContext): Future[WriteStatus] = {
    
    val nbytes = buffers.foldLeft(0)((sz, db) => sz + db.size)
    val writeEnd = offset + nbytes
    
    def begin(tailPath: List[IndexNode]): Future[WriteStatus] = {
      
      // returns remaining buffers and the offset at which they begin
      def updateExisting(): Future[(List[DataBuffer], Long)] = if (offset == 0) Future.successful((buffers, 0)) else {
        val ftail = synchronized {
          otail match {
            case Some(tail) => Future.successful(tail)
            case None =>
              val d = tailPath.head.entries.last
              read(d.pointer).map { dos => synchronized {
                val tail = Tail(d.offset, d.pointer, dos.revision, dos.data.size)
                otail = Some(tail)
                tail
              }}
          }
        }
        ftail.flatMap { tail =>
          val tailEnd = tail.offset + segmentSize
          val offsetInTail = (offset - tail.offset).asInstanceOf[Int]
          
          if (offset >= tailEnd) {
            Future.successful((buffers, offset))
          }
          else if (offsetInTail >= tail.size) {
            val appendBuffers = if (offsetInTail > tail.size) DataBuffer.zeroed(offsetInTail - tail.size) :: buffers else buffers
            val maxAppendSize = segmentSize - tail.size
            val (appendBuff, remaining) = DataBuffer.compact(maxAppendSize, appendBuffers)
            tx.append(tail.pointer, tail.revision, appendBuff)
            Future.successful((remaining, tail.offset + tail.size + appendBuff.size))
          } 
          else {
            read(tail.pointer).map(dos => updateSegment(tail.offset, dos, offset, buffers)) 
          }
        }
      }
      
      for {
        (remaining, appendOffset) <- updateExisting()
        allocated <- Future.sequence(recursiveAlloc(appendOffset, remaining, Nil))
        (newRoot, updatedNodes) <- IndexNode.rupdate(allocated.map(t => t._1), tailPath, tailPath.head, Nil)
      } yield {
        val fcomplete = tx.result.map { _ => synchronized {
          if (!allocated.isEmpty) {
            val (DownPointer(offset, pointer), size) = allocated.last
            otail = Some(Tail(offset, pointer, ObjectRevision(tx.uuid), size))
          } else {
            otail.foreach { tail =>
              val newSize = if (writeEnd > tail.offset + tail.size) (writeEnd - tail.offset).asInstanceOf[Int] else tail.size
              otail = Some(tail.copy(revision=ObjectRevision(tx.uuid), size = newSize))
            }
          }
          invalidateCachedNodes(updatedNodes)
        }}
        
        WriteStatus(Some(newRoot.pointer), 0, Nil, fcomplete)
      }
    }
    
    for {
      root <- getOrAllocateRoot(inode)
      tailPath <- root.getTail()
      fupdated <- begin(tailPath)
    } yield fupdated
  }
  
  /** A single write operation cannot add content to two index nodes in the same transaction. When this
   *  condition is encountered, only the data going into the first index node will be written. The 
   *  remaining data will be returned in the future. Subsequent write operations will be needed to finish
   *  writing the remaining data
   *  
   *  This write method creates DataObjects only at boundaries aligned to index.segmentSize in order to prevent
   *  backwards-writing applications from creating a large number of tiny file segments. 
   *  
   *  For writes spanning gaps (<obj> <gap> <obj>), update the first object and allocate to fill the gap but stop
   *  at the beginning of the next object? Should simplify impl
   *    
   */
   def write(
       inode: FileInode, 
       offset: Long, 
       buffers: List[DataBuffer])(implicit tx: Transaction, ec: ExecutionContext): Future[WriteStatus] = {
     
     if (offset == inode.size)
         writeTail(inode, offset, buffers)
     else {
       
       def updateContiguousRange(
           segmentSize: Long,
           beginOffset: Long, 
           remaining: List[DataBuffer], 
           entries: List[(DownPointer, IndexNode, DataObjectState)] ): (Long, List[DataBuffer]) = {
         
         require(beginOffset >= entries.head._1.offset && beginOffset < entries.head._1.offset + segmentSize)
         
         def rupdate(writeOffset: Long, toWrite: List[DataBuffer], elist: List[(DownPointer, IndexNode, DataObjectState)]): (Long, List[DataBuffer]) = {
           if (toWrite.isEmpty)
             (0, Nil)
           else if (elist.isEmpty)
             (writeOffset, toWrite)
           else if (writeOffset < elist.head._1.offset || writeOffset >= elist.head._1.offset + segmentSize)
             (writeOffset, toWrite)
           else {
             
             val (d, n, dos) = elist.head
             val nleft = toWrite.foldLeft(0)((sz, db) => sz + db.size)
             val objOffset = (writeOffset - d.offset).asInstanceOf[Int]
             val objSize = if (objOffset + nleft > dos.data.size) {
               if (objOffset + nleft > segmentSize) segmentSize else objOffset + nleft
             } else
               dos.size
               
             val bb = ByteBuffer.allocate(objSize.asInstanceOf[Int])
             bb.put(dos.data)
             
             val nwrite = if (objOffset + nleft < segmentSize) nleft else (segmentSize - objOffset)
             val (writeBuff, remaining) = if (objOffset + toWrite.head.size > bb.limit) {
               val (wb, rb) = toWrite.head.split(bb.limit - objOffset)
               (wb, rb :: toWrite.tail)
             } else {
               DataBuffer.compact(nwrite, toWrite)
             }
             
             bb.position(objOffset)
             bb.put(writeBuff)
             bb.position(0)
             //println(s"objOffset $objOffset, bufsize ${writeBuff.size} bb size ${bb.limit} content ${writeBuff.getByteArray().toList} remaining: ${remaining.size} elist: ${elist.size}")
             tx.overwrite(dos.pointer, dos.revision, bb)
             
             rupdate(writeOffset + nwrite, remaining, elist.tail)
           }
         }
         
         rupdate(beginOffset, remaining, entries)
       }
       
       def prepareWrite(headPath: List[IndexNode], entries: List[(DownPointer, IndexNode, DataObjectState)]): Future[WriteStatus] = {
         
         val segmentSize = headPath.head.index.segmentSize
         val beginObjectOffset = if (offset < segmentSize) 0 else offset - (offset.asInstanceOf[Int] % segmentSize) 
         
         def entryContains(d: DownPointer, tgtOffset: Long): Boolean = (tgtOffset >= d.offset && tgtOffset < d.offset + segmentSize)
         
         if (entries.isEmpty) {
           // pure allocation within a hole in the file
           
           val allocBuffers = if (beginObjectOffset == offset) buffers else DataBuffer.zeroed(offset - beginObjectOffset) :: buffers
           
           for {
             allocated <- Future.sequence(recursiveAlloc(beginObjectOffset, allocBuffers, Nil))
             (newRoot, updatedNodes) <- headPath.head.insert( allocated.map(t => t._1) )
           } yield {
             val fcomplete = tx.result.map( _ => invalidateCachedNodes(updatedNodes) )
             WriteStatus(Some(newRoot.pointer), 0, Nil, fcomplete)
           }
         }
         else if (!entryContains(entries.head._1, offset)) {
           // Write begins with an allocation in a hole and extends over at least one object
           val allocSize = (entries.head._1.offset - offset).asInstanceOf[Int]
           
           // returns (allocBuffers, remaining)
           def rgetAllocBuffers(nalloc: Int, remaining: List[DataBuffer], abuffs: List[DataBuffer]): (List[DataBuffer], List[DataBuffer]) = {
             val db = remaining.head
             
             if (nalloc == allocSize) {
               val allocBuffers = if (beginObjectOffset == offset) abuffs.reverse else DataBuffer.zeroed(offset - beginObjectOffset) :: abuffs.reverse
               (allocBuffers, remaining)
             }
             else if (nalloc + db.size <= allocSize)
               rgetAllocBuffers(nalloc + db.size, remaining.tail, db :: abuffs)
             else {
               val (a, b) = db.split(allocSize - nalloc)
               rgetAllocBuffers(nalloc + a.size, b :: remaining.tail, a :: abuffs)
             }
           }
           
           val (alloc, remaining) = rgetAllocBuffers(0, buffers, Nil)
           
           for {
             allocated <- Future.sequence(recursiveAlloc(beginObjectOffset, alloc, Nil))
             (newRoot, updatedNodes) <- headPath.head.insert( allocated.map(t => t._1) )
           } yield {
             val (remainingOffset, remainingData) = updateContiguousRange(segmentSize, entries.head._1.offset, remaining, entries)
             val fcomplete = tx.result.map( _ => invalidateCachedNodes(updatedNodes) )
             WriteStatus(Some(newRoot.pointer), remainingOffset, remainingData, fcomplete)
           }
         } else {
           // Write begins in an allocated segment
           val (remainingOffset, remainingData) = updateContiguousRange(segmentSize, offset, buffers, entries)
           
           Future.successful(WriteStatus(Some(headPath.last.pointer), remainingOffset, remainingData, tx.result.map(_=>())))
         }
       }
       
       val nbytes = buffers.foldLeft(0)((sz, db) => sz + db.size)
       
       for { 
         root <- getOrAllocateRoot(inode)
         (headPath, entries) <- root.getIndexEntriesForRange(offset, nbytes)
         objs <- Future.sequence(entries.map( t => fs.system.readObject(t._1.pointer).map{ dos => (t._1, t._2, dos) } )) 
         ftuple <- prepareWrite(headPath, objs)
       } yield ftuple
     }
   }
   
}

object IndexedFileContent {
  
  class FatalIndexError(msg: String) extends Exception(msg)
  
  class CorruptedIndex extends FatalIndexError("Corrupted Index")
  
  case class WriteStatus(newRoot: Option[DataObjectPointer], remainingOffset: Long, remainingData: List[DataBuffer], writeComplete: Future[Unit])
  
  private case class Tail(offset: Long, pointer: DataObjectPointer, revision: ObjectRevision, size: Int)
 
  private case class DownPointer(offset: Long, pointer: DataObjectPointer) {
    
    def encodedSize: Int = Varint.getUnsignedLongEncodingLength(offset) + pointer.encodedSize
    
    def encodeInto(bb: ByteBuffer): Unit = {
      Varint.putUnsignedLong(bb, offset)
      pointer.encodeInto(bb)
    }
    
    def encode(): DataBuffer = {
      val arr = new Array[Byte](encodedSize)
      val bb = ByteBuffer.wrap(arr)
      encodeInto(bb)
      DataBuffer(bb)
    }
  }
  
  private object DownPointer {
    def apply(bb: ByteBuffer): DownPointer = {
      val offset = Varint.getUnsignedLong(bb)
      val pointer = DataObjectPointer(bb)
      DownPointer(offset, pointer)
    }
  }
  
  object DeleteIndexTask {
    private val BaseKeyId = SteppedDurableTask.ReservedToKeyId
    
    val RootPointerKey = Key(BaseKeyId + 1)
    
    
    object TaskType extends DurableTaskType {
      
      val typeUUID: UUID = UUID.fromString("c1fb782f-7f13-4921-8ddf-155123445730")
     
      def createTask(
          system: AspenSystem, 
          pointer: DurableTaskPointer, 
          revision: ObjectRevision, 
          state: Map[Key, Value])(implicit ec: ExecutionContext): DurableTask = new DeleteIndexTask(system, pointer, revision, state)
    }
  }
  
  private def prepareIndexDeletionTask(
        fs: FileSystem,
        root: DataObjectPointer)(implicit tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] = {
         
    fs.localTaskGroup.prepareTask(DeleteIndexTask.TaskType, List((DeleteIndexTask.RootPointerKey, root.toArray))).map { _ => Future.unit }
      
  }
  
  /** Deletes an index. Note this this implementation is NOT for indicies with shared data. That would require
   *  exactly-once reference count decrements which this implementation does not currently enforce.
   */
  class DeleteIndexTask private (
      system: AspenSystem,
      pointer: DurableTaskPointer, 
      revision: ObjectRevision, 
      initialState: Map[Key, Value])(implicit ec: ExecutionContext)
         extends SteppedDurableTask(pointer, system, revision, initialState) {
    
    import DeleteIndexTask._
    
    def suspend(): Unit = {}
    
    def beginStep(): Unit = {
  
      def rdelete(nodePointer: DataObjectPointer): Future[Unit] = {
        system.readObject(nodePointer).flatMap { dos =>

          val (tier, startOffset, maxOffset, entries) = IndexNode.decode(dos.data)
          
          //println(s"Node ${nodePointer.uuid} tier ${tier} start $startOffset, max $maxOffset Lower Entries: ${entries.map(t => (t.offset -> t.pointer.uuid))}")
          // Delete lower entries
          val fl = entries.sortBy(e => e.offset).map { d =>
            system.retryStrategy.retryUntilSuccessful {
              if (tier > 0)
                rdelete(d.pointer)
              else {
                system.readObject(d.pointer).flatMap { sdos => 
                  implicit val tx = system.newTransaction()
                  //println(s"Deleting data node ${sdos.pointer.uuid}")
                  tx.setRefcount(sdos.pointer, sdos.refcount, sdos.refcount.decrement())
                  tx.commit()
                } recover {
                  case _: InvalidObject => () // already deleted
                  case t: Throwable => println(s"Unexpected error while deleting truncated data node ${d.pointer.uuid}: $t")
                }
              }
            }
          }
          
          // Delete self
          Future.sequence(fl.toList).flatMap { _ =>
            //println(s"Deleting index node ${nodePointer.uuid}")
            system.retryStrategy.retryUntilSuccessful {
              system.readObject(nodePointer).flatMap { sdos => 
                implicit val tx = system.newTransaction()
                tx.setRefcount(sdos.pointer, sdos.refcount, sdos.refcount.decrement())
                tx.commit()
              } recover {
                case _: InvalidObject => () // already deleted
                case t: Throwable => println(s"Unexpected error while deleting truncated index node: $t")
              }
            }
          }.map(_ => ())
        } recover {
          case _: InvalidObject => () // Already done!
        }
      }
      
      //println(s"******* STARTING FILE TRUNCATION TASK *****")
      system.retryStrategy.retryUntilSuccessful {
        rdelete(DataObjectPointer(state(RootPointerKey)))
      }.foreach { _ =>
        system.transactUntilSuccessful { tx =>
          //println(s"******* COMPLETED FILE TRUNCATION TASK *****")
          completeTask(tx)
          Future.unit
        }
      }
    }
  
  }
  
  private object IndexNode {
    def apply(
        pointer: DataObjectPointer,
        revision: ObjectRevision,
        data: DataBuffer,
        index: IndexedFileContent): IndexNode = {
      
      val (tier, offset, maxOffset, entries) = decode(data)
      
      new IndexNode(tier, index, pointer, revision, data.size, offset, maxOffset,
                    entries.sortBy(_.offset).toArray) 
    }
    
    def decode(data: DataBuffer): (Byte, Long, Option[Long], List[DownPointer]) = {
      val bb = data.asReadOnlyBuffer()
      val tier = bb.get()
      val offset = Varint.getUnsignedLong(bb)
      val rawMax = Varint.getUnsignedLong(bb)
      val maxOffset = if (rawMax == 0) None else Some(rawMax)
      
      var entries: List[DownPointer] = Nil
      
      while(bb.remaining() != 0) 
        entries = DownPointer(bb) :: entries

      (tier, offset, maxOffset, entries)
    }
    
    def getEncodedNodeContent(tier: Int = 0, content: List[DownPointer]=Nil, startOffset: Long = 0, endOffset: Option[Long]=None): DataBuffer = {
      val sz = 1 + Varint.getUnsignedLongEncodingLength(0) + Varint.getUnsignedLongEncodingLength(0) + content.foldLeft(0)((sz, e) => sz + e.encodedSize)
      val arr = new Array[Byte](sz)
      val bb = ByteBuffer.wrap(arr)
      bb.put(tier.asInstanceOf[Byte])
      Varint.putUnsignedLong(bb, startOffset)
      Varint.putUnsignedLong(bb, endOffset.getOrElse(0))
      content.foreach(e => e.encodeInto(bb))
      bb.position(0)
      DataBuffer(bb)
    }
    
    def prepareTruncation(
        endOffset: Long,
        path: List[IndexNode])(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
      
      val ftruncateData = path.head.getEntryForOffset(endOffset) match {
        case Some(d) => 
          if (endOffset < d.offset + path.head.index.segmentSize) {
            path.head.index.read(d.pointer).map { dos =>
              if (d.offset + dos.data.size > endOffset)
                tx.overwrite(dos.pointer, dos.revision, dos.data.slice(0, (endOffset-d.offset).asInstanceOf[Int]))
            }
          } else
            Future.unit
        case None => Future.unit
      }
      
      def rsplit(nodes: List[IndexNode], createdLowerNode: Option[DownPointer]): Future[DownPointer] = {
        if (nodes.isEmpty) 
          Future.successful(createdLowerNode.get)
        else {
          val node = nodes.head
          val (keep, discard) = node.entries.partition(e => e.offset <= endOffset)
          val newEntries = createdLowerNode match {
            case None => discard.toList
            case Some(e) => e :: discard.toList.filter(d => d.offset != e.offset) // replace original lower node with the new one
          }
          val updateContent = getEncodedNodeContent(node.tier, keep.toList, node.startOffset, None)
          val newContent = getEncodedNodeContent(node.tier, newEntries, endOffset, node.endOffset)
          
          tx.overwrite(node.pointer, node.revision, updateContent)
          
          node.index.allocateIndexNode(node.pointer, node.revision, tier=node.tier, content=newContent).flatMap { newPointer =>
            rsplit(nodes.tail, Some(new DownPointer(node.startOffset, newPointer)))
          }
        }
      }
      
      val fsplit = rsplit(path, None)
      
      for {
        _ <- ftruncateData
        truncatedRoot <- fsplit
        _ <- prepareIndexDeletionTask(path.head.index.fs, truncatedRoot.pointer)
      } yield()
    }
    
    /** Updates the head node of the supplied path and propagates index changes up the tree. 
     *  Returns: (NewRootNode, ListOfUpdatedNodes)
     */
    def rupdate(
        adds: List[DownPointer], 
        path: List[IndexNode], 
        last: IndexNode,
        updatedNodes: List[IndexNode])(implicit tx: Transaction, ec: ExecutionContext): Future[(IndexNode, List[IndexNode])] = {
      if (adds.isEmpty) {

        if (path.isEmpty)
          Future.successful((last, updatedNodes))
        else 
          rupdate(Nil, path.tail, path.head, updatedNodes)
      } 
      else {
        if (path.isEmpty) {
          // Allocate new root
          val oldRoot = updatedNodes.head
          
          val content = getEncodedNodeContent(oldRoot.tier + 1, new DownPointer(oldRoot.startOffset, oldRoot.pointer) :: adds)
          
          oldRoot.index.allocateIndexNode(oldRoot.pointer, oldRoot.revision, tier=0, content=content).map { newPointer =>
            val newRoot = IndexNode(newPointer, ObjectRevision(tx.uuid), content, oldRoot.index)
            (newRoot, newRoot :: updatedNodes)
          }
        } 
        else {
          val node = path.head
          
          val addSize = adds.foldLeft(0)((sz, e) => sz + e.encodedSize)
          //println(s"ADD INDEX SIZE: $addSize")
          if (node.haveRoomFor(addSize)) {
            val newEntries = (adds ++ node.entries.toList).sortBy(e => e.offset)
            
            val updated = new IndexNode(node.tier, node.index, node.pointer, ObjectRevision(tx.uuid), node.encodedSize + addSize, 
                                        node.startOffset, node.endOffset, newEntries.toArray)
            
            val arr = new Array[Byte](addSize)
            val bb = ByteBuffer.wrap(arr)
            adds.foreach(e => e.encodeInto(bb))
            
            //println(s"TX APPEND: Adds: ${adds}")
            tx.append(node.pointer, node.revision, DataBuffer(arr))
            
            rupdate(Nil, path.tail, path.head, updated :: updatedNodes)
          } 
          else {
            
            val allEntries = (adds ++ node.entries).sortBy(e => e.offset)
            
            val baseNodeSize = 33 // worst case base size for type byte and two Varint longs
            val maxNodeSize = node.index.tierNodeSize(node.tier)
            
            def rfill(entries: List[DownPointer], currentList: List[DownPointer], currentSize: Int, nodeContents: List[List[DownPointer]]): List[List[DownPointer]] = {
              if (entries.isEmpty)
                (currentList :: nodeContents).map(l => l.reverse).reverse
              else {
                val esize = entries.head.encodedSize
                if (currentSize + esize <= maxNodeSize)
                  rfill(entries.tail, entries.head :: currentList, currentSize + esize, nodeContents)
                else
                  rfill(entries.tail, entries.head :: Nil, baseNodeSize + esize, currentList :: nodeContents)
              }
            }
            
            val nodeContents = rfill(allEntries, Nil, baseNodeSize, Nil)
            
            val nodesToAllocate = nodeContents.tail
            
            val updateEnd = if (nodesToAllocate.isEmpty) node.endOffset else Some(nodesToAllocate.head.head.offset)
            
            val updateContent = getEncodedNodeContent(node.tier, nodeContents.head, node.startOffset, updateEnd)
            
            val txrev = ObjectRevision(tx.uuid)
            
            tx.overwrite(node.pointer, node.revision, updateContent)
            
            def allocNode(endOffset: Option[Long], newNodeEntries: List[DownPointer]): Future[IndexNode] = {
              val newNodeContent = getEncodedNodeContent(node.tier, newNodeEntries, newNodeEntries.head.offset, endOffset)
              node.index.allocateIndexNode(node.pointer, node.revision, tier=node.tier, content=newNodeContent).map { newPointer =>
                IndexNode(newPointer, txrev, newNodeContent, node.index)
              }
            }
            
            def rallocNodes(newNodes: List[List[DownPointer]], allocs: List[Future[IndexNode]]): List[Future[IndexNode]] = {
              if (newNodes.isEmpty)
                allocs
              else {
                val endOffset = if (newNodes.tail.isEmpty) node.endOffset else Some(newNodes.tail.head.head.offset)
                rallocNodes(newNodes.tail, allocNode(endOffset, newNodes.head) :: allocs)
              }
            }
            
            Future.sequence(rallocNodes(nodesToAllocate, Nil)).flatMap { allocatedNodes =>
              val updated = IndexNode(node.pointer, txrev, updateContent, node.index)
              rupdate(allocatedNodes.map(n => DownPointer(n.startOffset, n.pointer)), path.tail, path.head, updated :: (allocatedNodes ++ updatedNodes))
            }
          }
        }
      }
    }
  }
  
  private class IndexNode(
      val tier: Byte,
      val index: IndexedFileContent,
      val pointer: DataObjectPointer, 
      val revision: ObjectRevision,
      val dataSize: Int,
      val startOffset: Long,
      val endOffset: Option[Long],
      val entries: Array[DownPointer]
      ) {
    
    import IndexNode._
    
    def uuid: UUID = pointer.uuid
    
    def isTailNode: Boolean = endOffset.isEmpty
    
    def encodedSize: Int = {
      val loff = Varint.getUnsignedLongEncodingLength(startOffset)
      val lend = Varint.getUnsignedLongEncodingLength(endOffset.getOrElse(0))
      val lentries = entries.foldLeft(0)((sz, e) => sz + e.encodedSize)
      1 + loff + lend + lentries
    }
    
    def haveRoomFor(nbytes: Int): Boolean = {
      encodedSize + nbytes <= index.tierNodeSize(tier)
    }
    
    def getEntryForOffset(targetOffset: Long): Option[DownPointer] = {
      if (targetOffset < startOffset || endOffset.map(targetOffset > _).getOrElse(false))
        None
      else if (entries.length == 0)
        None
      else if (entries.length == 1)
        if (entries(0).offset <= targetOffset) Some(entries(0)) else None
      else {
        def rfind(idx: Int, last: Option[DownPointer]): Option[DownPointer] = {
          if (idx == entries.length || entries(idx).offset > targetOffset)
            last
          else
            rfind(idx+1, Some(entries(idx)))
        }
        rfind(1, Some(entries(0)))
      }
    }
    
    def getDownPointerForOffset(targetOffset: Long): Option[DownPointer] = getEntryForOffset(targetOffset) match {
      case Some(d: DownPointer) => Some(d)
      case _ => None
    }
    
    /** Seeks to the tier0 node owning the specified offset. ALL seeks must start from the root node.
     */
    def seek(targetOffset: Long, path: List[IndexNode]=Nil)(implicit ec: ExecutionContext): Future[List[IndexNode]] = {
      // If this node doesn't contain the target offset, we've hit an inconsistency during navigation
      // re-start from a refreshed root node
      if (targetOffset < startOffset || endOffset.map(targetOffset > _).getOrElse(false))
        index.refreshRoot().flatMap(newRoot => newRoot.seek(targetOffset))
      else {
        if (tier == 0)
          Future.successful(this :: path)
        else {
          getDownPointerForOffset(targetOffset) match {
            case Some(d) => index.load(d.pointer).flatMap(lowerNode => lowerNode.seek(targetOffset, this :: path))
            case None => Future.failed(new CorruptedIndex)
          }
        }
      }
    }
    
    def getTail(path: List[IndexNode]=Nil)(implicit ec: ExecutionContext): Future[List[IndexNode]] = {
      // If this node doesn't contain the target offset, we've hit an inconsistency during navigation
      // re-start from a refreshed root node
      if (!isTailNode)
        index.refreshRoot().flatMap(newRoot => newRoot.getTail())
      else {
        if (tier == 0)
          Future.successful(this :: path)
        else {
          entries.lastOption match {
            case Some(d: DownPointer) => index.load(d.pointer).flatMap(lowerNode => lowerNode.getTail(this :: path))
            case _ => Future.failed(new CorruptedIndex)
          }
        }
      }
    }
    
    /** Returns the path to the head IndexNode for the range and a list of the entries within the range along with which index node they
     *  are present within (range queries can potentially span multiple index nodes) 
     */
    def getIndexEntriesForRange(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[(List[IndexNode], List[(DownPointer, IndexNode)])] = {
      val seekOffset = if (offset >= index.segmentSize) {
        if (offset % index.segmentSize == 0) 
          offset
        else
          offset - index.segmentSize
      }
      else
        0
        
      val endOffset = offset + nbytes
      
      def rgetMore(headPath: List[IndexNode], node: IndexNode, lst: List[(DownPointer, IndexNode)]): Future[(List[IndexNode], List[(DownPointer, IndexNode)])] = {
        val updated = node.entries.foldLeft(lst)( (l, e) => if (e.offset >= seekOffset && e.offset < endOffset) (e, this) :: l else l )
        val done = node.endOffset match {
          case None => true
          case Some(nodeEnd) => nodeEnd >= endOffset
        }
        if (done) 
          Future.successful((headPath, updated.reverse)) 
        else {
          node.endOffset match {
            case Some(nodeEnd) => seek(nodeEnd).flatMap(right => rgetMore(headPath, right.head, updated))
            case None => getTail().flatMap(right => rgetMore(headPath, right.head, updated))
          }
        }
      }
      
      seek(seekOffset).flatMap(path => rgetMore(path, path.head, Nil))
    }
     
    def insert(newEntry: DownPointer)(implicit tx: Transaction, ec: ExecutionContext): Future[(IndexNode, List[IndexNode])] = {
      seek(newEntry.offset).flatMap(path => rupdate(List(newEntry), path, path.head, Nil))
    }
    
    def insert(newEntries: List[DownPointer])(implicit tx: Transaction, ec: ExecutionContext): Future[(IndexNode, List[IndexNode])] = {
      seek(newEntries.head.offset).flatMap(path => rupdate(newEntries, path, path.head, Nil))
    }
    
    def truncate(endOffset: Long)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
      seek(endOffset).flatMap(path => prepareTruncation(endOffset, path))
    }
    
  } // end IndexNode class
} // end IndexFileContent object