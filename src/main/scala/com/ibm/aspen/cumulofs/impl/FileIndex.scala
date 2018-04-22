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
      val lsize = Varint.getUnsignedIntEncodingLength(psize)
      1 + osize + psize + lsize
    }
    def encodeInto(bb: ByteBuffer): Unit = {
      bb.put(typeCode)
      Varint.putUnsignedLong(bb, offset)
      Varint.putUnsignedInt(bb, pointer.encodedSize)
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
        val psize = Varint.getUnsignedInt(bb)
        val pointer = DataObjectPointer(bb, Some(bb.position + psize))
        
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
  class IndexNode(val tier: Int, val index: FileIndex, initialContent: DecodedNodeContent) {
    
    val leftPointer = initialContent.leftPointer
    val segments = initialContent.segments
    private[this] var rptr = initialContent.rightPointer
    
    require(segments.size() >= 1)
    
    def isRootNode: Boolean = leftPointer.isEmpty
    
    def rightPointer: Option[RightPointer] = synchronized { rptr }
    
    def setRightPointer(r: RightPointer): Unit = synchronized { rptr = Some(r) }
    
    def headOffset: Long = segments.firstElement().offset
    def tailOffset: Long = segments.lastElement().offset
    
    def getSegment(offset: Long): Segment = {
      
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
  }
  
  
  trait IndexNodeCache {
    
    def getNode(tier: Int, entry: Entry): Option[IndexNode]
    
    def putNode(node: IndexNode): Unit
    
    def dropCache(): Unit

  }
  
  case class Root(tierLevel: Int, pointer: DataObjectPointer) {
    def toArray(): Array[Byte] = {
      val arr = new Array[Byte](Varint.getUnsignedLongEncodingLength(tierLevel) + pointer.encodedSize)
      val bb = ByteBuffer.wrap(arr)
      
      Varint.putUnsignedInt(bb, tierLevel)
      pointer.encodeInto(bb)
      
      arr
    }
  }
  object Root {
    def apply(arr: Array[Byte]): Root = {
      val bb = ByteBuffer.wrap(arr)
      val tierLevel = Varint.getUnsignedInt(bb)
      val pointer = DataObjectPointer(bb)
      Root(tierLevel, pointer)
    }
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
 */
class FileIndex(
    val fs: FileSystem,
    val cache: FileIndex.IndexNodeCache,
    initialInodeState: FileInode) {

  import FileIndex._
  
  val filePointer = initialInodeState.pointer
  
  private[this] val tierTails = new Vector[IndexNode]()
  private[this] var ofroot: Future[Option[Root]] = Future.successful(initialInodeState.fileIndexRoot().map(Root(_)))
  
  
  def loadIndexNode(tier: Int, entry: Entry)(implicit ec: ExecutionContext): Future[IndexNode] = cache.getNode(tier, entry) match {
    case Some(i) => Future.successful(i)
    case None => fs.system.readObject(entry.pointer) map { dos => 
      val node = new IndexNode(tier, this, Entry.decodeNodeContent(dos.data))
      cache.putNode(node)
      node
    }
  }
    
  def refreshRoot()(implicit ec: ExecutionContext): Future[Option[Root]] = synchronized {
    ofroot = fs.inodeLoader.load(filePointer) map ( _.asInstanceOf[FileInode] ) map { inode => 
      inode.fileIndexRoot() match {
        case None => 
          tierTails.removeAllElements()
          None
        case Some(arr) => Some(Root(arr))
      }
    }

    ofroot
  }
  
  
  /** Returns a serialized copy of the current root structure */
  //def indexRoot(): Array[Byte]
}