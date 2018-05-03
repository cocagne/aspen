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
import scala.collection.immutable.Queue
import com.ibm.aspen.base.ObjectAllocater
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer

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
      segments: Array[Segment], 
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
    override def toString(): String = s"$typeCode:$offset"
  }
  object Entry {
    
    def decodeNodeContent(db: DataBuffer): DecodedNodeContent = {
      var l: Option[LeftPointer] = None
      var s = List[Segment]()
      var r: Option[RightPointer] = None
      
      val bb = db.asReadOnlyBuffer()
      while (bb.remaining() != 0) {
        val typeCode = bb.get
        
        val offset = Varint.getUnsignedLong(bb)
        val pointer = DataObjectPointer(bb)
        
        typeCode match {
          case 0 => l = Some(new LeftPointer(offset, pointer))
          case 1 => s = new Segment(offset, pointer) :: s
          case 2 => r = Some(new RightPointer(offset, pointer))
        }
      }
      
      DecodedNodeContent(l, s.reverse.toArray, r)
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
  
  /** segments - List of (Segment, encodedSegmentSize) tuples */
  def encodeEntries(oleft: Option[LeftPointer], segments: List[(Segment, Int)], oright: Option[RightPointer], osize: Option[Int]): DataBuffer = {
    
    val size = osize.getOrElse(encodedEntriesSize(oleft, segments, oright))
    
    val lstr=oleft.map(_.toString).getOrElse("none")
    val rstr=oright.map(_.toString).getOrElse("none")
    println(s"Encoding: size $size left:${lstr} right:$rstr segs:${segments.map(_._1)}")
    
    val arr = new Array[Byte](size)
    val bb = ByteBuffer.wrap(arr)
    oleft.foreach(_.encodeInto(bb))
    segments.foreach(_._1.encodeInto(bb))
    oright.foreach(_.encodeInto(bb))
    DataBuffer(arr)
  }
  
  def encodedEntriesSize(oleft: Option[LeftPointer], segments: List[(Segment, Int)], oright: Option[RightPointer]): Int = {
    oleft.map(_.encodedSize).getOrElse(0) + segments.foldLeft(0)( (sz, t) => t._2 + sz ) + oright.map(_.encodedSize).getOrElse(0)
  }
  
  case class IndexNode(
      tier: Int, 
      index: FileIndex,
      nodePointer: DataObjectPointer,
      leftPointer: Option[LeftPointer], 
      segments: Array[Segment], 
      rightPointer: Option[RightPointer],
      revision: ObjectRevision,
      size: Int,
      timestamp: HLCTimestamp) {
    
    def isRootNode: Boolean = leftPointer.isEmpty 
    
    def headSegment: Segment = segments(0)
    def tailSegment: Segment = segments.last
    
    def headOffset: Long = headSegment.offset
    def tailOffset: Long = tailSegment.offset
    
    def containsOffset(offset: Long): Boolean = {
      val lessThanRight = rightPointer match { 
        case None => true
        case Some(rp) => offset < rp.offset
      }
      (headOffset <= offset && lessThanRight)
    }
    
    def refresh()(implicit ec: ExecutionContext): Future[IndexNode] = {
      index.cache.dropNode(this)
      index.loadIndexNode(tier, nodePointer)
    }
    
    def getSegmentContainingOffset(offset: Long): Segment = {
      
      // TODO: Switch to binary search rather than linear search
      @tailrec
      def rfind(i: Int): Segment = if (i == segments.size) {
        segments(segments.size-1)
      } else {
        if (segments(i).offset <= offset && (i == segments.length-1 || segments(i+1).offset > offset))
          segments(i)
        else
          rfind(i+1)  
      }
      
      rfind(0)
    }
    
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
    
    def dropNode(node: IndexNode): Unit = {}
    
    def putNode(node: IndexNode): Unit = {}

  }
  
  object NoCache extends IndexNodeCache
  
  
  /** Encoding of the index root is optimized for the two common operations of "read from the front" and "append data to the back".
   *  Even for very large files, the tree will be quite shallow so the tail node of each tier of the tree is maintained within the
   *  index root data structure. The "top" of the tree is always the last node in the serialized pointer array. When it is split,
   *  a new tier is formed by allocating a new node to hold pointers to it and the split node. A pointer to this new node is then
   *  appended to the array and written to the inode as part of the update transaction. Additionally, because opening files and
   *  reading from the front is so common, a pointer to the head tier0 node is stored in the root as well. 
   *  
   *  Note that tails must always contain at least one entry. During initial creation the tail will be equal to the head node.
   * 
   */
  case class Root(tier0head: DataObjectPointer, tails: Array[DataObjectPointer]) {
    require(tails.length > 0)
    
    def rootNode: DataObjectPointer = tails.last
    
    def toArray(): Array[Byte] = {
      val nbytes = tails.foldLeft(tier0head.encodedSize + rootNode.encodedSize)( (sz, p) => sz + p.encodedSize )
      val arr = new Array[Byte](nbytes)
      val bb = ByteBuffer.wrap(arr)
      tier0head.encodeInto(bb)

      // Single-element tail means that the head node is also the tail node. We can skip writing it
      if (tails.length > 1)
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
      
      val tailList = rdecode(Nil).reverse
      
      val tails = if( tailList.isEmpty ) Array(tier0head) else tailList.toArray
      
      Root(tier0head, tails)
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
  
  private def loadIndexNode(tier: Int, pointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[IndexNode] = synchronized {
    cache.getNode(tier, pointer) match {  
      case Some(i) => Future.successful(i)
      
      case None => loadingNodes.get(pointer.uuid) match {
        case Some(f) => f
        
        case None =>
          val fnode = fs.system.readObject(pointer) map { dos => 
            val dnc = Entry.decodeNodeContent(dos.data)
            println(s"LoadingIndex Node. Tier:$tier Left:${dnc.leftPointer} Right:${dnc.rightPointer} Seg:${dnc.segments.toList}")
            val node = IndexNode(tier, this, pointer, dnc.leftPointer, dnc.segments, dnc.rightPointer, dos.revision, dos.size, dos.timestamp)
            cache.putNode(node)
            node
          }
          loadingNodes += (pointer.uuid -> fnode)
          fnode andThen { case _ => synchronized { loadingNodes -= pointer.uuid } }
          fnode
      }
    }
  }
  
  def getIndexNodeForOffset(offset: Long)(implicit ec: ExecutionContext): Future[Option[IndexNode]] = synchronized {
    println(s"Getting index node for offset: $offset")
    foRoot.flatMap { oroot => oroot match { 
      case None => Future.successful(None)
      case Some(root) => 
        if (offset == 0)
          loadIndexNode(0, root.tier0head).map(Some(_))
        else {
          loadIndexNode(root.tails.length-1, root.tails.last) flatMap { indexRoot =>
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
                
                val dnc = Entry.decodeNodeContent(dos.data)
                val node = IndexNode(tier, this, dos.pointer, dnc.leftPointer, dnc.segments, dnc.rightPointer, dos.revision, dos.size, dos.timestamp)
                
                cache.putNode(node)
                v.add(node)
              }
    
              v
            }
        }}
        
        ofTails = Some(ftails)
        
        ftails
    }
  }
  
  private def simpleAppend(
      tier: Int, 
      tails: Vector[IndexNode], 
      segments: List[(Segment, Int)], 
      encodedSize: Int)(implicit tx: Transaction, ec: ExecutionContext): IndexNode = {
        
    val tail = tails.get(tier)

    tx.append(tail.nodePointer, tail.revision, encodeEntries(None, segments, None, Some(encodedSize)))
    
    tail.copy(
          segments = tail.segments ++ segments.map(_._1), 
          revision = tx.txRevision, 
          size = tail.size + encodedSize,
          timestamp = tx.timestamp())
  }
      
  private def overwriteSplitTail(
      maxNodeSize: Int,
      oldTail: DataObjectPointer, 
      oldRevision: ObjectRevision, 
      oldLeft: Option[LeftPointer], 
      remainingSegments: List[(Segment, Int)],
      newTailPointer: DataObjectPointer)(implicit tx: Transaction, ec: ExecutionContext): List[(Segment, Int)] = {
    
    def nextRightPointerSize(lseg: List[(Segment, Int)]): Int = lseg.tail.headOption match {
      case None => 0
      case Some(t) => new RightPointer(t._1.offset, newTailPointer).encodedSize
    }
    
    @tailrec
    def rfill(entries: List[Entry], size: Int, lseg: List[(Segment, Int)]): (List[Entry], List[(Segment, Int)]) = {
      if (lseg.isEmpty)
        (entries.reverse, lseg)
      else {
        if (size + lseg.head._2 + nextRightPointerSize(lseg) > maxNodeSize) {
          ((new RightPointer(lseg.head._1.offset, newTailPointer) :: entries).reverse, lseg)
        } else
          rfill(lseg.head._1 :: entries, size + lseg.head._2, lseg.tail)
      }
    }
    
    val (entries, leftover) = oldLeft match {
      case None => rfill(Nil, 0, remainingSegments)
      case Some(lp) =>
        val (e, l) = rfill(Nil, lp.encodedSize, remainingSegments)
        (lp :: e, l)
    }

    val encodedSize = entries.foldLeft(0)( (sz, e) => e.encodedSize + sz )
    val arr = new Array[Byte](encodedSize)
    val bb = ByteBuffer.wrap(arr)
    
    entries.foreach(_.encodeInto(bb))
    println(s"Overwriting split tail. Entries ${entries} remaining: ${leftover.map(_._1)}")
    tx.overwrite(oldTail, oldRevision, DataBuffer(arr))
    
    leftover
  }
  
  private def splitingAppend(
      tier: Int,
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision,
      startTail: DataObjectPointer, 
      startRevision: ObjectRevision, 
      tails: Vector[IndexNode], 
      segments: List[(Segment, Int)])(implicit tx: Transaction, ec: ExecutionContext): Future[(List[(Segment, Int)], IndexNode)] = {
    
    val maxNodeSize = fs.getDataTableNodeSize(tier)
    
    def rappend(
        allocater: ObjectAllocater,
        tail: DataObjectPointer, 
        tailOffset: Long,
        tailRevision: ObjectRevision, 
        tailLeft: Option[LeftPointer],
        appendedNodes: List[(Segment, Int)],
        segList: List[(Segment, Int)]): Future[(List[(Segment, Int)], IndexNode)] = {
      
      val size = encodedEntriesSize(tailLeft, segList, None)
      
      if (size <= maxNodeSize) {
        println(s"tier $tier Overwrite Calculated size: $size. for ${segList.map(_._1)}")  
        tx.overwrite(tail, tailRevision, encodeEntries(tailLeft, segList, None, Some(size)))
        
        val newTail = new IndexNode(tier, this, tail, tailLeft, segList.map(_._1).toArray, None, tx.txRevision, size, tx.timestamp)
        
        Future.successful((appendedNodes.reverse, newTail))
      } else {
        allocater.allocateDataObject(allocatingObject, allocatingObjectRevision, DataBuffer.Empty) flatMap { newTailPointer =>
          val remainingSegments = overwriteSplitTail(maxNodeSize, tail, tailRevision, tailLeft, segList, newTailPointer)
          val left = new LeftPointer(tailOffset, tail)
          val tailSeg = new Segment(remainingSegments.head._1.offset, newTailPointer)
          println(s"tier $tier Recursing! with total segments ${segList.length} remaining segments: ${remainingSegments.length}")
          rappend(allocater, newTailPointer, segList.head._1.offset, tx.txRevision, Some(left), (tailSeg, tailSeg.encodedSize) :: appendedNodes, remainingSegments)
        }
      }
    }
    
    fs.getDataTableNodeAllocater(tier) flatMap { allocater =>
      rappend(allocater, startTail, segments.head._1.offset, startRevision, tails.get(tier).leftPointer, Nil, segments)
    }
  }
  
  private def appendAndPropagateUp(
        allocatingObject: ObjectPointer,
        allocatingObjectRevision: ObjectRevision,
        tier: Int, 
        tails: Vector[IndexNode], 
        segments: List[(Segment, Int)],
        newTails: List[IndexNode])(implicit tx: Transaction, ec: ExecutionContext): Future[List[IndexNode]] = {

    if (tier == tails.size) {
      val headSeg = new Segment(0,tails.lastElement().nodePointer) 
      val rootContent = (headSeg, headSeg.encodedSize) :: segments
      println(s"Creating new root for teir $tier. Segments: ${rootContent.map(_._1)}")
      val encodedSegmentSize = encodedEntriesSize(None, rootContent, None)
      val newRootData = encodeEntries(None, rootContent, None, Some(encodedSegmentSize))
      
      for {
        allocater <- fs.getDataTableNodeAllocater(tier) 
        rootPointer <- allocater.allocateDataObject(allocatingObject, allocatingObjectRevision, newRootData)
      } yield {
        
        val newRootNode = IndexNode(tier, this, rootPointer, None, segments.map(_._1).toArray, None, tx.txRevision, encodedSegmentSize, tx.timestamp())
        
        newRootNode :: newTails
      }
    } else {
      println(s"appendAndPropagate up for tier $tier")
      val startTail = tails.get(tier)
      val maxNodeSize = fs.getDataTableNodeSize(tier)
      val encodedSegmentSize = segments.foldLeft(0)( (sz, t) => t._2 + sz )
      
      if (startTail.size + encodedSegmentSize <= maxNodeSize)
          Future.successful(simpleAppend(tier, tails, segments, encodedSegmentSize) :: newTails)
      else {
        splitingAppend(tier, allocatingObject, allocatingObjectRevision, startTail.nodePointer, startTail.revision, tails, segments) flatMap { t =>
          val (nodeSegments, newTail) = t
          appendAndPropagateUp(allocatingObject, allocatingObjectRevision, tier+1, tails, nodeSegments, newTail :: newTails)
        }
      }
    }
  }
  /** data - Sequence of (pointer, objectSize)
   *  Returns future to transaction prepared for commit. Return value is the updated Root node for inclusion into the 
   *  inode on successful transaction commit and a Future to all index nodes having been updated in response
   *  to a successful transaction commit. This future must complete before the next append call is made or there will
   *  be a race condition that could lead to transaction failures.
   */
  def prepareAppend(
      inodeObjectPointer: KeyValueObjectPointer,
      inodeObjectRevision: ObjectRevision,
      inodeTimestamp: HLCTimestamp,
      currentFileSize: Long,
      data: List[(DataObjectPointer, Int)])(implicit tx: Transaction, ec: ExecutionContext): Future[(Root, Future[Unit])] = {
    
    tx.ensureHappensAfter(inodeTimestamp)
    
    def convertToSegmentAndEncodedSizes(
        d: List[(DataObjectPointer, Int)], 
        nextOffset: Long, 
        segments: List[(Segment, Int)]): List[(Segment, Int)] = if (d.isEmpty) segments.reverse else {
      val s = new Segment(nextOffset, d.head._1)
      convertToSegmentAndEncodedSizes(d.tail, nextOffset + d.head._2, (s, s.encodedSize) :: segments) 
    }
    
    val newSegments = convertToSegmentAndEncodedSizes(data, currentFileSize, Nil)
    
    val pStateUpdated = Promise[Unit]()
    
    // Reread the inode and tails if tx fails so we're prepared for a retry
    tx.result.failed.foreach { cause =>
      pStateUpdated.failure(cause)
      refresh()
    }
    
    def prep(oroot: Option[Root], tails: Vector[IndexNode]): Future[(Root, ()=>Unit)] = oroot match {
      case None => // allocate initial root 
        for {
          allocater <- fs.getDataTableNodeAllocater(0) 
          tier0tail <- allocater.allocateDataObject(inodeObjectPointer, inodeObjectRevision, DataBuffer.Empty)
          
          root = new Root(tier0tail, Array(tier0tail))
          
          _ = tails.add(IndexNode(0, this, tier0tail, None, Array(), None, tx.txRevision, 0, tx.timestamp()))
          
          // Recursively call with new root & tails vector
          result <- prep(Some(root), tails)
        } yield {
          result
        }
        
      case Some(root) =>
        
        appendAndPropagateUp(inodeObjectPointer, inodeObjectRevision, 0, tails, newSegments, Nil) map { reversedNewTails =>
          
          @tailrec
          def updateTails(lst: List[DataObjectPointer], tier: Int): Array[DataObjectPointer] = {
            if (tier >= root.tails.length)
              lst.reverse.toArray
            else
              updateTails(root.tails(tier) :: lst, tier+1)
          }

          val newTails = reversedNewTails.reverse
          val rnewTailPointers = reversedNewTails.map(_.nodePointer)
          
          val newRoot = new Root(root.tier0head, updateTails(rnewTailPointers, rnewTailPointers.length))
          
          def updateIndexStateOnSuccess(): Unit = {
            foRoot = Future.successful(Some(newRoot))
            
            newTails.zipWithIndex.foreach { t =>
              if (t._2 == tails.size)
                tails.add(t._1)
              else {
                cache.dropNode(tails.get(t._2))
                tails.set(t._2, t._1)
              }
            }
          }
          
          (newRoot, updateIndexStateOnSuccess _)
        }
    }
    
    for {
      tails <- getTails()
      oroot <- foRoot
      (newRoot, updateIndexState) <- prep(oroot, tails)
    } yield {
      
      tx.result foreach { case _ => synchronized {
        updateIndexState()
        pStateUpdated.success(())
      }}
      
      (newRoot, pStateUpdated.future)
    }
  }
  
  
}