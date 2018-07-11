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

class IndexedFileContent(val fs: FileSystem, inode: FileInode) {
  import IndexedFileContent._
  
  private val inodePointer = inode.pointer
  
  private def refreshRoot()(implicit ec: ExecutionContext): Future[IndexNode] = {
    
    dropAllCachedNodes()
    
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
        dropAllCachedNodes()
        
        val content = IndexNode.getEncodedNodeContent()
        
        allocateIndexNode(inode.pointer.pointer, inode.revision, tier=0, content=content).map { newPointer =>
          IndexNode(newPointer, ObjectRevision(tx.uuid), content, this)
        }
        
      case Some(inodeRootArr) => load(DataObjectPointer(inodeRootArr))
    }
  }
  
  private val segmentSize = fs.defaultSegmentSize
  
  private def tierNodeSize(tier: Int): Int = fs.getDataTableNodeSize(tier)
  
  private def read(pointer: DataObjectPointer): Future[DataObjectState] = fs.system.readObject(pointer)
  
  private val cache: Cache[UUID, IndexNode] = Scaffeine().maximumSize(50).build[UUID, IndexNode]()
  
  private def dropAllCachedNodes(): Unit = cache.invalidateAll()
  
  private def updateCachedNodes(l: List[IndexNode]): Unit = l.foreach(n => cache.put(n.uuid, n))
  
  private def load(nodePointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[IndexNode] = {
    cache.getIfPresent(nodePointer.uuid) match {
      case Some(n) => Future.successful(n)
      case None => fs.system.readObject(nodePointer).flatMap { dos =>
        val n = IndexNode(dos.pointer, dos.revision, dos.data, this)
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
        
        load(DataObjectPointer(arr)).flatMap { node =>
          node.getIndexEntriesForRange(offset, nbytes).flatMap { segments =>
            val fbufs = Future.sequence(segments.map { t => t._1 match {
              case h: Hole => Future.successful(Left(h))
              case d: DownPointer => read(d.pointer).map( dos => Right((d.offset, dos.data)) )
            }})
            
            fbufs.map { ebufs =>
              ebufs.foreach { e => e match {
                case Left(_) =>
                case Right((doffset, db)) =>
                  bb.position((doffset - offset).asInstanceOf[Int])
                  val slice = if (bb.position + db.size > bb.limit) db.slice(0, bb.limit - bb.position) else db
                  bb.put(slice)
              }}
              
              bb.position(0)
              Some(DataBuffer(bb))
            }
          }
        }
    }
  }
  
  /** A single write operation cannot add content to two index nodes in the same transaction. When this
   *  condition is encountered, only the data going into the first index node will be written. The 
   *  remaining data will be returned in the future. Subsequent write operations will be needed to finish
   *  writing the remaining data
   *  
   *  The outter future completes when the transaction is ready to commit. The inner future completes when the
   *  internal index structure has been successfully updated. The resulting tuple contains the offset of the
   *  remaining data that couldn't be written along with the buffers containing that data.
   */
   def write(
       inode: FileInode, 
       offset: Long, 
       buffers: List[DataBuffer])(implicit tx: Transaction, ec: ExecutionContext): Future[Future[(Long, List[DataBuffer])]] = {
     
     
     def prepareWrite(alignedOffset: Long, entries: List[(IndexEntry, IndexNode)]): Future[Future[(Long, List[DataBuffer])]] = {
       
     }
     
     val nbytes = buffers.foldLeft(0)((sz, db) => sz + db.size)
     
     for { 
       root <- getOrAllocateRoot(inode)
       remainder = offset % root.index.segmentSize
       (alignedOffset, seekBytes) = if (offset >= root.index.segmentSize) {
         val remainder = offset.asInstanceOf[Int] % root.index.segmentSize
         (offset - remainder, nbytes+remainder)
       } 
       else (0L, nbytes + offset.asInstanceOf[Int])
       entries <- root.getIndexEntriesForRange(alignedOffset,  seekBytes)
       ftuple <- prepareWrite(alignedOffset, entries)
     } yield ftuple
   }
}

object IndexedFileContent {
  
  class FatalIndexError(msg: String) extends Exception(msg)
  
  //class HolesNotSupported extends FatalIndexError("Holes Not Supported")
  
  class CorruptedIndex extends FatalIndexError("Corrupted Index")
  
  /** 
   *  transactionPrepared - Completes when all operations have been added to the transaction. The returned value is the new 
   *                        file size and root index pointer
   *  operationComplete - Completes after all caches and internal state have been updated to reflect a successful transaction commit 
   */
  case class Update(transactionPrepared: Future[(Long, DataObjectPointer)], operationComplete: Future[Unit])
  
  private sealed abstract class IndexEntry {
    val typeCode: Byte
    val offset: Long
    def encodedSize: Int
    def encodeInto(bb: ByteBuffer): Unit
    def encode(): DataBuffer = {
      val arr = new Array[Byte](encodedSize)
      val bb = ByteBuffer.wrap(arr)
      encodeInto(bb)
      DataBuffer(bb)
    }
  }
  
  private object IndexEntry {
    def decode(bb: ByteBuffer): IndexEntry = {
      bb.get() match {
        case 0 => 
          val offset = Varint.getUnsignedLong(bb)
          val pointer = DataObjectPointer(bb)
          new DownPointer(offset, pointer)
        case 1 =>
          val offset = Varint.getUnsignedLong(bb)
          new Hole(offset)
      }
    }
  }
 
  private class DownPointer(val offset: Long, val pointer: DataObjectPointer) extends IndexEntry {
    
    val typeCode: Byte = 0
    
    def encodedSize: Int = 1 + Varint.getUnsignedLongEncodingLength(offset) + pointer.encodedSize
    
    def encodeInto(bb: ByteBuffer): Unit = {
      bb.put(typeCode)
      Varint.putUnsignedLong(bb, offset)
      pointer.encodeInto(bb)
    }
  }
  
  private class Hole(val offset: Long) extends IndexEntry {
    
    val typeCode: Byte = 1
    
    def encodedSize: Int = 1 + Varint.getUnsignedLongEncodingLength(offset)
    
    def encodeInto(bb: ByteBuffer): Unit = {
      bb.put(typeCode)
      Varint.putUnsignedLong(bb, offset)
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
          
          // Delete lower entries
          val fl = entries.sortBy(e => e.offset).map { e =>
            e match {
              case h: Hole => Future.unit
              case d: DownPointer =>
                system.retryStrategy.retryUntilSuccessful {
                  if (tier > 0)
                    rdelete(d.pointer)
                  else {
                    system.readObject(d.pointer).flatMap { sdos => 
                      implicit val tx = system.newTransaction()
                      tx.setRefcount(sdos.pointer, sdos.refcount, sdos.refcount.decrement())
                      tx.commit()
                    } recover {
                      case _: InvalidObject => () // already deleted
                      case t: Throwable => println(s"Unexpected error while deleting truncated nodes: $t")
                    }
                  }
                }
            }
          }
          
          // Delete self
          Future.sequence(fl.toList).flatMap { _ =>
            println(s"Deleting index node ${nodePointer.uuid}")
            system.retryStrategy.retryUntilSuccessful {
              system.readObject(nodePointer).flatMap { sdos => 
                implicit val tx = system.newTransaction()
                tx.setRefcount(sdos.pointer, sdos.refcount, sdos.refcount.decrement())
                tx.commit()
              } recover {
                case _: InvalidObject => () // already deleted
                case t: Throwable => println(s"Unexpected error: $t")
              }
            }
          }
        } recover {
          case _: InvalidObject => () // Already done!
        }
      }
      
      println(s"******* STARTING FILE TRUNCATION TASK *****")
      system.retryStrategy.retryUntilSuccessful {
        rdelete(DataObjectPointer(state(RootPointerKey)))
      }.foreach { _ =>
        system.transactUntilSuccessful { tx =>
          println(s"******* COMPLETED FILE TRUNCATION TASK *****")
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
    
    def decode(data: DataBuffer): (Byte, Long, Option[Long], List[IndexEntry]) = {
      val bb = data.asReadOnlyBuffer()
      val tier = bb.get()
      val offset = Varint.getUnsignedLong(bb)
      val rawMax = Varint.getUnsignedLong(bb)
      val maxOffset = if (rawMax == 0) None else Some(rawMax)
      
      var entries: List[IndexEntry] = Nil
      
      while(bb.remaining() != 0) 
        entries = IndexEntry.decode(bb) :: entries
      
      (tier, offset, maxOffset, entries)
    }
    
    def getEncodedNodeContent(tier: Int = 0, content: List[IndexEntry]=Nil, startOffset: Long = 0, endOffset: Option[Long]=None): DataBuffer = {
      val sz = 1 + Varint.getUnsignedLongEncodingLength(0) + Varint.getUnsignedLongEncodingLength(0) + content.foldLeft(0)((sz, e) => sz + e.encodedSize)
      val arr = new Array[Byte](sz)
      val bb = ByteBuffer.wrap(arr)
      bb.put(tier.asInstanceOf[Byte])
      Varint.putUnsignedLong(bb, startOffset)
      Varint.putUnsignedLong(bb, endOffset.getOrElse(0))
      content.foreach(e => e.encodeInto(bb))
      DataBuffer(bb)
    }
    
    def prepareTruncation(
        endOffset: Long,
        path: List[IndexNode])(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
      
      val ftruncateData = path.head.getEntryForOffset(endOffset) match {
        case Some(d: DownPointer) => path.head.index.read(d.pointer).map { dos =>
          if (d.offset + dos.data.size > endOffset)
            tx.overwrite(dos.pointer, dos.revision, dos.data.slice(0, (endOffset-d.offset).asInstanceOf[Int]))
        }
        case Some(h: Hole) => Future.unit
        case None => Future.failed(new CorruptedIndex)
      }
      
      def rsplit(nodes: List[IndexNode], createdLowerNode: Option[DownPointer]): Future[DownPointer] = {
        if (nodes.isEmpty) 
          Future.successful(createdLowerNode.get)
        else {
          val node = nodes.head
          val (keep, discard) = node.entries.partition(e => e.offset < endOffset)
          val newEntries = createdLowerNode match {
            case None => discard.toList
            case Some(e) => e :: discard.toList
          }
          val updateContent = getEncodedNodeContent(node.tier, keep.toList, node.startOffset, None)
          val newContent = getEncodedNodeContent(node.tier, newEntries, endOffset, node.endOffset)
          
          node.index.allocateIndexNode(node.pointer, node.revision, tier=node.tier, content=newContent).flatMap { newPointer =>
            rsplit(nodes.tail, Some(new DownPointer(endOffset, newPointer)))
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
        
    sealed abstract class IndexOperation {
      val e: IndexEntry
    }
    
    class Add(val e: IndexEntry) extends IndexOperation
    class Del(val e: IndexEntry) extends IndexOperation
    
    def rupdate(
        ops: List[IndexOperation], 
        path: List[IndexNode], 
        updatedNodes: List[IndexNode])(implicit tx: Transaction, ec: ExecutionContext): Future[(IndexNode, List[IndexNode])] = {
      if (ops.isEmpty) {
        if (path.tail.isEmpty) 
          Future.successful((path.head, updatedNodes)) 
        else 
          rupdate(Nil, path.tail, updatedNodes)
      } 
      else {
        if (path.isEmpty) {
          // Allocate new root
          val oldRoot = updatedNodes.head
          
          val content = getEncodedNodeContent(oldRoot.tier + 1, new DownPointer(oldRoot.startOffset, oldRoot.pointer) :: ops.map(op => op.e))
          
          oldRoot.index.allocateIndexNode(oldRoot.pointer, oldRoot.revision, tier=0, content=content).map { newPointer =>
            val newRoot = IndexNode(newPointer, ObjectRevision(tx.uuid), content, oldRoot.index)
            (newRoot, newRoot :: updatedNodes)
          }
        } 
        else {
          val node = path.head
          
          val (adds, dels) = ops.foldLeft((List[IndexEntry](), List[IndexEntry]())) { (t, op) => op match {
            case add: Add => (add.e :: t._1, t._2)
            case del: Del => (t._1, del.e :: t._2)
          }}
          
          val onlyAdds = dels.isEmpty
          val addSize = adds.foldLeft(0)((sz, e) => sz + e.encodedSize)
          
          if (onlyAdds && node.haveRoomFor(addSize)) {
            val newEntries = (node.entries.toList ++ adds).sortBy(e => e.offset)
            
            val updated = new IndexNode(node.tier, node.index, node.pointer, ObjectRevision(tx.uuid), node.encodedSize + addSize, 
                                        node.startOffset, node.endOffset, newEntries.toArray)
            
            val arr = new Array[Byte](addSize)
            val bb = ByteBuffer.wrap(arr)
            adds.foreach(e => e.encodeInto(bb))
            
            tx.append(node.pointer, node.revision, DataBuffer(arr))
            
            rupdate(Nil, path.tail, updated :: updatedNodes)
          } 
          else {
            
            val delSet = dels.map(e => e.offset).toSet
            
            val allEntries = (adds ++ node.entries.toList.filter(e => !delSet.contains(e.offset))).sortBy(e => e.offset)
            
            val baseNodeSize = 33 // worst case base size for type byte and two Varint longs
            val maxNodeSize = node.index.tierNodeSize(node.tier)
            
            def rfill(entries: List[IndexEntry], currentList: List[IndexEntry], currentSize: Int, nodeContents: List[List[IndexEntry]]): List[List[IndexEntry]] = {
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
            
            def allocNode(endOffset: Option[Long], newNodeEntries: List[IndexEntry]): Future[IndexNode] = {
              val newNodeContent = getEncodedNodeContent(node.tier, newNodeEntries, newNodeEntries.head.offset, endOffset)
              node.index.allocateIndexNode(node.pointer, node.revision, tier=node.tier, content=newNodeContent).map { newPointer =>
                IndexNode(newPointer, txrev, newNodeContent, node.index)
              }
            }
            
            def rallocNodes(newNodes: List[List[IndexEntry]], allocs: List[Future[IndexNode]]): List[Future[IndexNode]] = {
              if (newNodes.isEmpty)
                allocs
              else {
                val endOffset = if (newNodes.tail.isEmpty) node.endOffset else Some(newNodes.tail.head.head.offset)
                rallocNodes(newNodes.tail, allocNode(endOffset, newNodes.head) :: allocs)
              }
            }
            
            Future.sequence(rallocNodes(nodesToAllocate, Nil)).flatMap { allocatedNodes =>
              val updated = IndexNode(node.pointer, txrev, updateContent, node.index)
              rupdate(allocatedNodes.map(n => new Add(new DownPointer(n.startOffset, n.pointer))), path.tail, updated :: (allocatedNodes ++ updatedNodes))
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
      val entries: Array[IndexEntry]
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
    
    def getEntryForOffset(targetOffset: Long): Option[IndexEntry] = {
      if (targetOffset < startOffset || endOffset.map(targetOffset > _).getOrElse(false))
        None
      else if (entries.length == 0)
        None
      else if (entries.length == 1)
        if (entries(0).offset <= targetOffset) Some(entries(0)) else None
      else {
        def rfind(idx: Int, last: Option[IndexEntry]): Option[IndexEntry] = {
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
    
    def getIndexEntriesForRange(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[List[(IndexEntry, IndexNode)]] = {
      val seekOffset = if (offset >= index.segmentSize) {
        if (offset % index.segmentSize == 0) 
          offset
        else
          offset - index.segmentSize
      }
      else
        0
        
      val endOffset = offset + nbytes
      
      def rgetMore(node: IndexNode, lst: List[(IndexEntry, IndexNode)]): Future[List[(IndexEntry, IndexNode)]] = {
        val updated = entries.foldLeft(lst)( (l, e) => if (e.offset >= seekOffset && e.offset < endOffset) (e, this) :: l else l )
        val done = node.endOffset match {
          case None => true
          case Some(nodeEnd) => nodeEnd >= endOffset
        }
        if (done) 
          Future.successful(lst.reverse) 
        else {
          node.endOffset match {
            case Some(nodeEnd) => seek(nodeEnd).flatMap(right => rgetMore(right.head, lst))
            case None => getTail().flatMap(right => rgetMore(right.head, lst))
          }
        }
      }
      
      seek(seekOffset).flatMap(path => rgetMore(path.head, Nil))
    }
     
    def insert(newEntry: IndexEntry)(implicit tx: Transaction, ec: ExecutionContext): Future[(IndexNode, List[IndexNode])] = {
      seek(newEntry.offset).flatMap(path => rupdate(List(new Add(newEntry)), path, Nil))
    }
    
    def truncate(endOffset: Long)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
      seek(endOffset).flatMap(path => prepareTruncation(endOffset, path))
    }
    
  } // end IndexNode class
} // end IndexFileContent object