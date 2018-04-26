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
import com.ibm.aspen.core.HLCTimestamp

/** FileIndex is similar to a TieredList in that it uses a hierarchy of singly-linked lists to build a distributed 
 *  index but this implementation is optimized specifically for indexing file data. Data is sequentially appended to
 *  files as they grow and the data offsets are fixed at time of allocation. Consequently, there is no advantage to
 *  the typical B-tree model of maintaining partially populated nodes and no need for supporting efficient splits of anything
 *  but the right-most node in the list. Applications are also not typically written such that multiple processes
 *  attempt to simultaneously append to the same file so contention will generally not be a concern for transactions
 *  maintaining the tree structure. Whereas the TieredList implementation depends on FinalizationActions for maintaining
 *  the tree structure, this implementation performs those activities directly within the context of split/append
 *  transactions.
 *  
 *  For files with "holes" in them, mid-tree nodes may eventually need to be split to support writes to the "hole" space.
 *  These will generally bubble up the tree structure to the root node and will result in a somewhat less efficient tree
 *  structure but it will function correctly. 
 *  
 *  Future Notes:
 *  
 *     Embedded Inode Data
 *        * Direct content embedding in root (for very small files)
 *        * Direct tier0 embedding (up to 4 or 5 tier0 nodes for fairly small files)
 *        
 *     Multiple Writers
 *        * Appends always overwrite the Inode (cause file size is changing too)
 *        * For mid-file writers, maintain a separate object for the mtime and have an "active writers" array
 *          embedded in the inode. Mid-file writes update the state for that writer only
 *          - Readers of inode are then responsible for deducing the current mtime
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
  
  
  
  class IndexNode(
      val tier: Int, 
      val index: FileIndex,
      val nodePointer: DataObjectPointer, 
      private[this] var revision: ObjectRevision,
      private[this] var size: Int,
      private[this] var timestamp: HLCTimestamp, 
      initialContent: DecodedNodeContent) {
    
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
          val f = index.fs.system.readObject(nodePointer) map { dos => 
            refresh(dos)
            this
          }
          ofrefresh = Some(f)
          f.andThen { case _ => synchronized { ofrefresh = None } }
          f
      }
    }
    
    /** Refreshes the node state IFF the provided state is newer than the current state */
    def refresh(currentState: DataObjectState): Unit = synchronized {
      if (currentState.timestamp.compareTo(timestamp) > 0) {
        val dnc = Entry.decodeNodeContent(currentState.data)
        lptr = dnc.leftPointer
        rptr = dnc.rightPointer
        segments.clear()
        segments.addAll(dnc.segments)
        revision = currentState.revision
        timestamp = currentState.timestamp
        size = currentState.size
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
      case Some(lp) => index.loadIndexNode(tier, lp.pointer).map(Some(_))
    }
    def fetchRight()(implicit ec: ExecutionContext): Future[Option[IndexNode]] = rightPointer match {
      case None => Future.successful(None)
      case Some(rp) => index.loadIndexNode(tier, rp.pointer).map(Some(_))
    }
    def fetchTier0Node(offset: Long)(implicit ec: ExecutionContext): Future[IndexNode] = {
      if (tier == 0 && containsOffset(offset))
        Future.successful(this)
      else if (tier == 0)
        seekTo(offset)
      else 
        seekTo(offset) flatMap { node => 
          index.loadIndexNode(tier-1, getSegmentContainingOffset(offset).pointer) flatMap { lowerNode =>
            lowerNode.fetchTier0Node(offset)
          }
        }
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
    
    def dropCache(): Unit = {}
    
    def getNode(tier: Int, pointer: DataObjectPointer): Option[IndexNode] = None
    
    protected def putNode(node: IndexNode): Unit = {}
    
    def refresh(tier: Int, index: FileIndex, dos: DataObjectState): IndexNode = synchronized {
      getNode(tier, dos.pointer) match { 
        case Some(node) => 
          node.refresh(dos)
          node
          
        case None =>
          val node = new IndexNode(tier, index, dos.pointer, dos.revision, dos.size, dos.timestamp, Entry.decodeNodeContent(dos.data))
          putNode(node)
          node
      }
    }
  }
  
  
  /** Encoding of the index root is optimized for the two common operations of "read from the front" and "append data to the back".
   *  Even for very large files, the tree will be quite shallow so the tail node of each tier of the tree is maintained within the
   *  index root data structure. The "top" of the tree is always the last node in the serialized pointer array. When it is split,
   *  a new tier is formed by allocating a new node to hold pointers to it and the split node. A pointer to this new node is then
   *  appended to the array and written to the inode as part of the update transaction. Additionally, because opening files and
   *  reading from the front is so common, a pointer to the head tier0 node is stored in the root as well. 
   * 
   */
  case class Root(tier0head: DataObjectPointer, tails: Array[DataObjectPointer]) {
    def toArray(): Array[Byte] = {
      val nbytes = tails.foldLeft(tier0head.encodedSize)( (sz, p) => sz + p.encodedSize )
      val arr = new Array[Byte](nbytes)
      val bb = ByteBuffer.wrap(arr)
      tier0head.encodeInto(bb)
      tails.foreach(_.encodeInto(bb))
      arr
    } 
  }
  object Root {
    def apply(arr: Array[Byte]): Root = {
      val bb = ByteBuffer.wrap(arr)
      
      val tier0head = DataObjectPointer(bb)
      
      def rdecode(l: List[DataObjectPointer]): List[DataObjectPointer] = if (bb.remaining == 0) l else {
        rdecode( DataObjectPointer(bb) :: l )
      }
      
      Root(tier0head, rdecode(Nil).reverse.toArray)
    }
  }
}

/**
 * 
 */
class FileIndex(
    val fs: FileSystem,
    val cache: FileIndex.IndexNodeCache,
    initialInodeState: FileInode) {

  import FileIndex._
  
  val filePointer = initialInodeState.pointer
  
  private[this] var foRoot: Future[Option[Root]] = Future.successful(initialInodeState.fileIndexRoot().map(Root(_)))
  private[this] var ofTails: Option[Future[Vector[IndexNode]]] = None
  private[this] var loadingNodes = Map[UUID, Future[IndexNode]]()
  
  def loadIndexNode(tier: Int, pointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[IndexNode] = synchronized {
    cache.getNode(tier, pointer) match {  
      case Some(i) => Future.successful(i)
      
      case None => loadingNodes.get(pointer.uuid) match {
        case Some(f) => f
        
        case None =>
          val fnode = fs.system.readObject(pointer) map ( dos => cache.refresh(tier, this, dos) )
          loadingNodes += (pointer.uuid -> fnode)
          fnode andThen { case _ => synchronized { loadingNodes -= pointer.uuid } }
          fnode
      }
    }
  }
  
  def getIndexNodeForOffset(offset: Long)(implicit ec: ExecutionContext): Future[Option[IndexNode]] = synchronized {
    foRoot.flatMap { oroot => oroot match { 
      case None => Future.successful(None)
      case Some(root) => 
        if (offset == 0)
          loadIndexNode(0, root.tier0head).map(Some(_))
        else {
          loadIndexNode(root.tails.length-1, root.tails(root.tails.length-1)) flatMap { indexRoot =>
            indexRoot.fetchTier0Node(offset).map(Some(_))
          }
        }
    }}
  }
  
  def refresh()(implicit ec: ExecutionContext): Future[Option[Root]] = synchronized {
    ofTails = None
    
    foRoot = fs.inodeLoader.load(filePointer) map { inode => synchronized {
      inode.fileIndexRoot() match {
        case None => None
        case Some(arr) => Some(Root(arr))
      }
    }}

    foRoot
  }
  
  def getTails()(implicit ec: ExecutionContext): Future[Vector[IndexNode]] = synchronized {
    ofTails match {
      case Some(f) => f
      case None =>
        val ftails = foRoot flatMap { oroot => oroot match {
          case None => Future.successful(new Vector[IndexNode](2,1))
          
          case Some(root) => 
      
            val fstate = Future.sequence(root.tails.map(fs.system.readObject(_)).toList)
              
            fstate map { tailStates =>
              val v = new Vector[IndexNode](root.tails.length+1, 1)
              
              tailStates.zipWithIndex.foreach { t =>
                val (dos, tier) = t
                
                v.add( cache.refresh(tier, this, dos) )
              }
    
              v
            }
        }}
        
        ofTails = Some(ftails)
        ftails
    }
  }
  
  /** Returns future to transaction prepared for commit. Result is a tuple containing the new Root object and
   *  the number of segments appended. The number of appended segments may be less than the total number provided.
   */
  def prepareAppend(segments: Seq[Segment])(implicit t: Transaction, ec: ExecutionContext): Future[(Root, Int)] = ???
  
  /** Returns a serialized copy of the current root structure */
  //def indexRoot(): Array[Byte]
}