package com.ibm.aspen.cumulofs.impl

import com.ibm.aspen.core.objects.DataObjectPointer
import java.util.UUID
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.util.Varint
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectRevision
import scala.concurrent.Future
import scala.annotation.tailrec
import com.ibm.aspen.base.ObjectReader
import java.util.Vector
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.cumulofs.FileSystem
import com.ibm.aspen.cumulofs.FilePointer
import com.ibm.aspen.cumulofs.FileInode
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.base.Transaction

/** FileIndex is similar to a TieredList in that it uses a hierarchy of singly-linked lists to build a distributed 
 *  index but this implementation is optimized specifically for indexing file data. Data is sequentially appended to
 *  files as they grow and the data offsets are fixed at time of allocation. Consequently, there is no advantage to
 *  the typical B-tree model of maintaining partially populated nodes and no need for supporting splits of anything
 *  but the right-most node in the list. Applications are also not typically written such that multiple processes
 *  attempt to simultaneously append to the same file so contention will generally not be a concern for transactions
 *  maintaining the tree structure. Whereas the TieredList implementation depends on FinalizationActions for maintaining
 *  the tree structure, this implementation performs those activities directly within the context of split/append
 *  transactions.
 * 
 *  Index could be a simple list of Entry until a size is exceeded. Then it could switch to a tier structure
 *  
 *  TODO: Truncation - Single TX. Find all tiers at trunc point. Copy post-trunc Entries to newly-allocated nodes. Create
 *                     task to delete the tree of entries
 */
object FileIndex {
  
  case class DecodedNodeContent(
      leftPointer: Option[LeftPointer], 
      segments: Vector[Segment], 
      rightPointer: Option[RightPointer])
  
  sealed abstract class Entry {
    def typeCode: Byte
    val offset: Long
    val pointer: DataObjectPointer
    
    def encodedSize: Int = {
      val osize = Varint.getUnsignedLongEncodingLength(offset)
      val psize = pointer.encodedSize
      1 + osize + psize
    }
    def encodeInto(bb: ByteBuffer): Unit = {
      bb.put(typeCode)
      Varint.putUnsignedLong(bb, offset)
      pointer.encodeInto(bb)
    }
    def toArray(): Array[Byte] = {
      val arr = new Array[Byte](encodedSize)
      encodeInto(ByteBuffer.wrap(arr))
      arr
    }
  }
  object Entry {
    
    def decodeNodeContent(db: DataBuffer): DecodedNodeContent = {
      var l: Option[LeftPointer] = None
      val s = new Vector[Segment]()
      var r: Option[RightPointer] = None
      
      val bb = db.asReadOnlyBuffer()
      while (bb.remaining() != 0) {
        val typeCode = bb.get
        
        val offset = Varint.getUnsignedLong(bb)
        val pointer = DataObjectPointer(bb)
        
        typeCode match {
          case 0 => l = Some(new LeftPointer(offset, pointer))
          case 1 => s.add(new Segment(offset, pointer))
          case 2 => r = Some(new RightPointer(offset, pointer))
        }
      }
      
      DecodedNodeContent(l, s, r)
    }
  }
  class LeftPointer(val offset: Long, val pointer: DataObjectPointer) extends Entry {
    override def typeCode: Byte = 0
  }
  class Segment(val offset: Long, val pointer: DataObjectPointer) extends Entry {
    override def typeCode: Byte = 1  
  }
  class RightPointer(val offset: Long, val pointer: DataObjectPointer) extends Entry {
    override def typeCode: Byte = 2
  }
  
  
  
  /* The only node who's size or revision we care about its the tails
   * We may care about caching segments for search/replace
   * Arrays are probably faster for iteration purposes.
   * Every append to tier will also include an inode update in the transaction
   * can map successful commit to updating tier tail pointers.
   * 
   * Following through, every index update involves an inode update. That also means there's no point in doing
   * concurrent transactions. This probably ought to be serialized at the File level. Multiple handlers can refer
   * to the same file. Requests are then serialized through an operation request queue.
   *   * Allows file to buffer multiple writes till 4Mb limit is reached or fsync is called.
   *   * Overwrite, Append, & Truncate are the only real operations we have
   *   
   * All changes involve changing the mtime so we'll need to serialize all operations to the file. Maintaining
   * a buffer of outstanding operations should allow batching of operations. Prep for the next Tx can be done
   * while the current one is underway.
   * 
   * File could maintain currentTx, nextTx, and nextTxReadyForCommit. If currentTx completes successfully and nextTxReadyForCommit is true,
   * immediately commit next Tx w/ successfully completed txUUID as the required revision. A simple opPrepCounter is used to identify each
   * "prepare" operation. If the completion of a prepare matches the current counter value nextTxReadyForCommit is flipped to true and the
   * pendingOps value is incremented. Next op placed in buffer, look at pendingOps. If above threshold, leave in queue. If below, incr prepCounter,
   * pendingOps, and set nextTxReadyForCommit to false. 
   * 
   * On Tx failure, invalidate nextTx and move all current and pending ops back into the operations queue then re-start as if the ops were
   * freshly arrived.
   * 
   * So, FileIndex needs to be a self-contained mutable object. We'll only allow a single append call per Tx so we can correctly handle
   * node splits
   * 
   * 
   * Inode - Could just store FileIndex Root + Tier Depth
   * File - getCachedIndexNode(uuid)
   * 
   * getTails(): Future[Array[Tail]] - build tails list via recursive function. 
   * invalidateTails(): sets cached future to None subsequent getTails() re-builds the list. call on any tx failure
   * 
   */
  class IndexNode(val tier: Int, val index: FileIndex, val nodePointer: DataObjectPointer, initialContent: DecodedNodeContent) {
    
    private[this] var lptr = initialContent.leftPointer
    private[this] var rptr = initialContent.rightPointer
    private[this] var ofrefresh: Option[Future[IndexNode]] = None
    
    // Vectors are synchronized
    val segments = initialContent.segments
    
    require(segments.size() >= 1)
    
    def isRootNode: Boolean = synchronized { lptr.isEmpty }
    
    def leftPointer: Option[LeftPointer] = synchronized { lptr }
    def setLeftPointer(l: LeftPointer): Unit = synchronized { lptr = Some(l) }
    
    def rightPointer: Option[RightPointer] = synchronized { rptr }
    def setRightPointer(r: RightPointer): Unit = synchronized { rptr = Some(r) }
    
    def headOffset: Long = segments.firstElement().offset
    def tailOffset: Long = segments.lastElement().offset
    
    def containsOffset(offset: Long): Boolean = {
      val lessThanRight = rightPointer match { 
        case None => true
        case Some(rp) => offset < rp.offset
      }
      (headOffset <= offset && lessThanRight)
    }
    
    def refresh()(implicit ec: ExecutionContext): Future[IndexNode] = synchronized {
      ofrefresh match {
        case Some(f) => f
        case None =>
          val f = index.fs.system.readObject(nodePointer) map { dos => synchronized {
            val dnc = Entry.decodeNodeContent(dos.data)
            lptr = dnc.leftPointer
            rptr = dnc.rightPointer
            segments.clear()
            segments.addAll(dnc.segments)
            this
          }}
          ofrefresh = Some(f)
          f
      }
    }
    
    def getSegmentContainingOffset(offset: Long): Segment = {
      
      // TODO: Switch to binary search rather than linear search
      @tailrec
      def rfind(i: Int): Segment = if (i == segments.size) {
        segments.get(segments.size-1)
      } else {
        if (segments.get(i).offset <= offset && segments.get(i+1).offset > offset)
          segments.get(i)
        else
          rfind(i+1)  
      }
      
      rfind(0)
    }
    
    def tailSegment: Segment = segments.lastElement()
    
    def fetchLeft()(implicit ec: ExecutionContext): Future[Option[IndexNode]] = leftPointer match {
      case None => Future.successful(None)
      case Some(lp) => index.loadIndexNode(tier, lp).map(Some(_))
    }
    def fetchRight()(implicit ec: ExecutionContext): Future[Option[IndexNode]] = rightPointer match {
      case None => Future.successful(None)
      case Some(rp) => index.loadIndexNode(tier, rp).map(Some(_))
    }
    def seekTo(offset: Long)(implicit ec: ExecutionContext): Future[IndexNode] = {
      if (containsOffset(offset)) 
        Future.successful(this) 
      else {
        val p = Promise[IndexNode]()
        def rsearch(node: IndexNode): Unit = if (node.containsOffset(offset)) p.success(this) else {
          node.fetchRight() onComplete {
            case Failure(cause) => p.failure(cause)
            case Success(onode) => onode match {
              case Some(node) => rsearch(node)
              case None => p.failure(new Exception("Navigation Error"))
            }
          }
        }
        def lsearch(node: IndexNode): Unit = if (node.containsOffset(offset)) p.success(this) else {
          node.fetchLeft() onComplete {
            case Failure(cause) => p.failure(cause)
            case Success(onode) => onode match {
              case Some(node) => lsearch(node)
              case None => p.failure(new Exception("Navigation Error"))
            }
          }
        }
        if (offset < headOffset) lsearch(this) else rsearch(this)
        p.future
      }
    }
  }
  
  trait IndexNodeCache {
    
    def getNode(tier: Int, entry: Entry): Option[IndexNode]
    
    def putNode(node: IndexNode): Unit
    
    def dropCache(): Unit

  }
  
  class Tail(
      var node: IndexNode,
      var revision: ObjectRevision,
      var size: Int)
  
  /** Encoding of the index root is optimized for the two common operations of "read from the front" and "append data to the back".
   *  Even for very large files, the tree will be quite shallow so the tail node of each tier of the tree is maintained within the
   *  index root data structure. The "top" of the tree is always the last node in the serialized pointer array. When it is split,
   *  a new tier is formed by allocating a new node to hold pointers to it and the split node. A pointer to this new node is then
   *  appended to the array and written to the inode as part of the update transaction. Additionally, because opening files and
   *  reading from the front is so common, a pointer to the head tier0 node is stored in the root as well. 
   * 
   */
  case class Root(tier0head: DataObjectPointer, tails: Array[DataObjectPointer]) {
    def toArray(): Array[Byte] = ???
      /*
    {
      val lroot = rootPointer match {
        case None => 0
        case Some(rp) => rp.encodedSize
      }
      val lhead = tier0head.encodedSize
      val ltail = tier0tail.encodedSize
      
      val szroot = Varint.getUnsignedIntEncodingLength(lroot)
      val szhead = Varint.getUnsignedIntEncodingLength(lhead)
      val sztail = Varint.getUnsignedIntEncodingLength(ltail)
      
      val arr = new Array[Byte](1 + 1 + szroot + szhead + sztail)
      val bb = ByteBuffer.wrap(arr)
      
      bb.put(0.asInstanceOf[Byte]) // encoding format
      bb.put(tierLevel)
      Varint.putUnsignedInt(bb, lroot)
      Varint.putUnsignedInt(bb, lhead)
      Varint.putUnsignedInt(bb, ltail)
      rootPointer.foreach(_.encodeInto(bb))
      tier0head.encodeInto(bb)
      tier0tail.encodeInto(bb)
      
      arr
    } */
  }
  object Root {
    def apply(arr: Array[Byte]): Root = ???
    /* {
      val bb = ByteBuffer.wrap(arr)
      
      val encodingFormat = bb.get()
      val tierLevel = bb.get()
      val rootLen = Varint.getUnsignedInt(bb)
      val headLen = Varint.getUnsignedInt(bb)
      val tailLen = Varint.getUnsignedInt(bb)
      
      val rootPointer = if (rootLen == 0) None else Some(DataObjectPointer(bb, Some(rootLen)))
      val tier0head = DataObjectPointer(bb, Some(headLen))
      val tier0tail = DataObjectPointer(bb, Some(tailLen))
      
      Root(tierLevel, rootPointer, tier0head, tier0tail)
    } */
  }
}

/**
 * 
 * Could also add an entryType byte where entries are Either[Segment, LeftPointer, RightPointer]
 *    - Compatible with appends
 *    - Once a RightPointer is set, the node is closed
 *    - Makes navigation WAY easier
 *    - Prevents need to always have an up-to-date root
 *    - Allows independent impl of sequential cursor for forward/backward scan
 *       - upper tiers can be used for seek opz
 *    - Makes storing tier0 in inode attractive again
 *    - Same with tail. Only when a split occurs do we need to search up
 *        - This should be infrequent enough that we don't need to cache it. Do full search from root on each split
 *            - can opz later
 *        - This HUGELY simplifies the impl
 *        
 *        
 * Tree handling. 3 common use cases: 
 *    1. Get first byte for sequential read (HUGELY COMMON)
 *    2. Get tail for append
 *    3. Random Seek within file
 * 
 * 1 & 2 solved by embedding tier0 pointers in inode
 * 
 * There will be so few tiers... what if we just put all tier tails in the root pointer?
 *   The "root" is always the tail of the highest tier. Once it splits, the tail of the next tier up becomes the root.
 *       * SIMPLE! Yay!
 *   Root = tier0 head, [tier0tail, tier1tail]
 */
class FileIndex(
    val fs: FileSystem,
    val cache: FileIndex.IndexNodeCache,
    initialInodeState: FileInode) {

  import FileIndex._
  
  val filePointer = initialInodeState.pointer
  
  private[this] var ofroot: Future[Option[Root]] = Future.successful(initialInodeState.fileIndexRoot().map(Root(_)))
  private[this] var otail: Option[Tail] = None
  
  def loadIndexNode(tier: Int, entry: Entry)(implicit ec: ExecutionContext): Future[IndexNode] = cache.getNode(tier, entry) match {
    case Some(i) => Future.successful(i)
    case None => fs.system.readObject(entry.pointer) map { dos => 
      val node = new IndexNode(tier, this, entry.pointer, Entry.decodeNodeContent(dos.data))
      cache.putNode(node)
      node
    }
  }
  /*
  def refreshRoot()(implicit ec: ExecutionContext): Future[Option[Root]] = synchronized {
    ofroot = fs.inodeLoader.load(filePointer) map ( _.asInstanceOf[FileInode] ) map { inode => synchronized {
      inode.fileIndexRoot() match {
        case None => 
          otail = None
          None
          
        case Some(arr) => 
          val r = Root(arr)
          otail.foreach { tail =>
            if (r.tier0tail != tail)
              otail = None
          }
          Some(r)
      }
    }}

    ofroot
  } */
  
  def prepareAppend(segments: Seq[Segment])(implicit t: Transaction, ec: ExecutionContext): Future[Root] = ???
  
  /** Returns a serialized copy of the current root structure */
  //def indexRoot(): Array[Byte]
}