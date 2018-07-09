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
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.read.InvalidObject

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
  
  case class DataTail(pointer: DataObjectPointer, revision: ObjectRevision, offset: Long, size: Int)
  
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
    
    val arr = new Array[Byte](size)
    val bb = ByteBuffer.wrap(arr)
    oleft.foreach(_.encodeInto(bb))
    segments.foreach(_._1.encodeInto(bb))
    oright.foreach(_.encodeInto(bb))
    DataBuffer(arr)
  }
  
  def getSegmentSizes(segments: List[Segment]): List[(Segment, Int)] = segments.map(s => (s, s.encodedSize))
  
  def encodedEntriesSize(oleft: Option[LeftPointer], segments: List[(Segment, Int)], oright: Option[RightPointer]): Int = {
    oleft.map(_.encodedSize).getOrElse(0) + segments.foldLeft(0)( (sz, t) => t._2 + sz ) + oright.map(_.encodedSize).getOrElse(0)
  }
  
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

  private[cumulofs] def destroyNodeSegment(node: IndexNode)(implicit ec: ExecutionContext): Future[IndexNode] = if (node.segments.isEmpty) Future.successful(node) else {
    val victim = node.segments.last
    
    val fready = if (node.tier == 0) {
      Future.unit
    } else {
      node.index.loadIndexNode(node.tier-1, victim.pointer).flatMap(lowerNode => lowerNode.destroy()) recover {
        case o: InvalidObject => ()
      }
    }
    
    fready.flatMap { _ =>
      implicit val tx = node.index.fs.system.newTransaction()
      
      def getRemaining(i: Int, l: List[(Segment, Int)]): List[(Segment, Int)] = if (i == -1) l else {
        getRemaining(i-1, (node.segments(i), node.segments(i).encodedSize) :: l)
      }
      val remaining = getRemaining(node.segments.length-2, Nil)
      val newIndexNodeContent = encodeEntries(node.leftPointer, remaining, node.rightPointer, None)
      
      if (node.tier == 0) 
        tx.setRefcount(victim.pointer, ObjectRefcount(0,1), ObjectRefcount(1,0))
      
      tx.overwrite(node.nodePointer, node.revision, newIndexNodeContent)
      tx.commit().map { _ =>
        IndexNode(node.tier, node.index, node.nodePointer, node.leftPointer, remaining.map(t=>t._1).toArray, node.rightPointer,
            tx.txRevision, node.size, tx.timestamp())
      }
    }
  }
  
  private[cumulofs] def destroyNodeSegments(node: IndexNode)(implicit ec: ExecutionContext): Future[IndexNode] = if (node.segments.isEmpty) 
    Future.successful(node) 
  else {
    destroyNodeSegment(node).flatMap(newNode => destroyNodeSegments(newNode)) 
  }
  
  private[cumulofs] def destroyTruncatedTree(
      system: AspenSystem, 
      tier: Int, 
      root: DataObjectPointer)(implicit ec: ExecutionContext): Future[Unit] = {
    println(s"destroyTruncatedTree tier $tier node id ${root.uuid}")
    system.readObject(root).flatMap { dos =>
      println(s"destroyTruncatedTree got node id ${root.uuid}")
      val dnc = decodeNodeContent(dos.data)
      val segments = dnc.segments.sortWith((a,b) => a.offset < b.offset).reverse
      
      val fl = segments.map { s => system.retryStrategy.retryUntilSuccessful {
        
        if (tier == 0) {
          println(s"deleting tier 0 node: ${s.pointer.uuid}")
          system.readObject(s.pointer).flatMap { sdos => 
            implicit val tx = system.newTransaction()
            tx.setRefcount(s.pointer, sdos.refcount, sdos.refcount.decrement())
            tx.commit()
          } recover {
            case _: InvalidObject => () // already deleted
            case t: Throwable => println(s"Unexpected error: $t")
          }
        } else {
          println("RECURSING! ")
          destroyTruncatedTree(system, tier-1, s.pointer)
        }
      }}
      println(s"Segment Deletion Futures Created")
      Future.sequence(fl.toList).flatMap { _ =>
        // Delete self
        println(s"Deleting root node ${root.uuid}")
        system.retryStrategy.retryUntilSuccessful {
          system.readObject(root).flatMap { sdos => 
            implicit val tx = system.newTransaction()
            tx.setRefcount(root, sdos.refcount, sdos.refcount.decrement())
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
    
    def getSegmentsForRange(offset: Long, nbytes: Int)(implicit ec: ExecutionContext): Future[List[Segment]] = {
      val tailOffset = offset + nbytes
      
      def rfill(n: IndexNode, lst: List[Segment]): Future[List[Segment]] = {
        val updatedList = segments.filter(s => s.offset >= offset && s.offset < tailOffset) ++: lst
        
        if (containsOffset(offset) || leftPointer.isEmpty) {
          val lst = if (updatedList.isEmpty || offset != updatedList.head.offset ) {
            getSegmentContainingOffset(offset) :: updatedList
          } else {
            updatedList
          }
          
          Future.successful(lst)
        } else
          fetchLeft().flatMap(rn => rfill(rn.get, updatedList))
      }
      
      seekTo(tailOffset).flatMap(rfill(_, Nil)) 
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
          index.loadIndexNode(tier-1, node.getSegmentContainingOffset(offset).pointer) flatMap { lowerNode =>
            lowerNode.fetchTier0Node(offset)
          }
        }
    }
    /** Splits the index into two indicies at the specified offset and returns the root node of the index with updated
     *  tail pointers and a pointer to the head of the index structure containing the to-be-deleted index and data nodes.
     */
    def prepareTruncation(root: Root, offset: Long)(implicit tx: Transaction, ec: ExecutionContext): Future[(Option[Root], DataObjectPointer)] = {
      
      def trace(node: IndexNode, path: List[IndexNode]): Future[List[IndexNode]] = {
        if (tier == 0)
          node.seekTo(offset).map(target => target :: path)
        else 
          node.seekTo(offset) flatMap { target => 
            index.loadIndexNode(tier-1, target.getSegmentContainingOffset(offset).pointer) flatMap { lowerNode =>
              trace(lowerNode, target :: path)
            }
          }
      }
      
      def prepDataSegment(tier0: IndexNode): Future[Unit] = {
        val s = tier0.getSegmentContainingOffset(offset)
        index.fs.system.readObject(s.pointer) map { kvos =>
          if (s.offset < offset && !(s.offset + kvos.data.size != offset))
            tx.overwrite(s.pointer, kvos.revision, kvos.data.slice(0, (offset - s.offset).asInstanceOf[Int]))
        }
      }
      
      def splitIndexNode(node: IndexNode, splitBelow: Option[DataObjectPointer]): Future[DataObjectPointer] = {
        val (skeep, sdiscard) = node.segments.toList.partition(s => s.offset < offset)
        val rightSegments = splitBelow match {
          case None => sdiscard
          case Some(p) => new Segment(offset, p) :: sdiscard
        }
        
        index.cache.dropNode(node)
        tx.overwrite(nodePointer, revision, encodeEntries(leftPointer, getSegmentSizes(skeep), None, None))
        
        index.fs.getDataTableNodeAllocater(tier) flatMap { allocater =>
          allocater.allocateDataObject(nodePointer, revision, encodeEntries(None, getSegmentSizes(rightSegments), rightPointer, None))
        }
      }
      
      def splitUp(path: List[IndexNode], lastSplitObject: DataObjectPointer): Future[DataObjectPointer] = {
        if (path.isEmpty)
          Future.successful(lastSplitObject)
        else {
          splitIndexNode(path.head, Some(lastSplitObject)) flatMap { splitObject =>
            splitUp(path.tail, splitObject)
          }
        }
      }
      
      for {
        path <- trace(this, Nil)
        _ <- prepDataSegment(path.head)
        tier0split <- splitIndexNode(path.head, None)
        truncatedRoot <- splitUp(path.tail, tier0split)
      } yield {
        val oroot = if (offset == 0) None else Some(Root(root.tier0head, path.map(_.nodePointer).toArray))
        (oroot, truncatedRoot)
      }
    }
    
    def seekTo(offset: Long)(implicit ec: ExecutionContext): Future[IndexNode] = {
      if (containsOffset(offset))
        Future.successful(this) 
      else {
        
        val p = Promise[IndexNode]()
        def rsearch(node: IndexNode): Unit = if (node.containsOffset(offset)) p.success(node) else {
          node.fetchRight() onComplete {
            case Failure(cause) => p.failure(cause)
            case Success(onode) => onode match {
              case Some(node) => rsearch(node)
              case None => p.failure(new Exception("Navigation Error"))
            }
          }
        }
        def lsearch(node: IndexNode): Unit = if (node.containsOffset(offset)) p.success(node) else {
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
    
    /** Future returns when all segments in the node have been destroyed */
    private[cumulofs] def destroySegments()(implicit ec: ExecutionContext): Future[IndexNode] = {
      
      @volatile var node = this
      
      def onAttemptFailure(cause: Throwable): Future[Unit] = {
        refresh() map { updatedNode =>
          node = updatedNode
        }
      }
      
      index.fs.system.retryStrategy.retryUntilSuccessful(onAttemptFailure _) {
        destroyNodeSegments(node)
      }
    }
    
    private[cumulofs] def destroy()(implicit ec: ExecutionContext): Future[Unit] = index.fs.system.retryStrategy.retryUntilSuccessful {
      destroySegments() flatMap { node =>   
        implicit val tx = index.fs.system.newTransaction()
        tx.setRefcount(node.nodePointer, ObjectRefcount(0,1), ObjectRefcount(1,0))
        tx.commit()
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
      if (tails.length == 1) 
        tier0head.toArray
      else { 
        val nbytes = tails.foldLeft(tier0head.encodedSize)( (sz, p) => sz + p.encodedSize )
        val arr = new Array[Byte](nbytes)
        val bb = ByteBuffer.wrap(arr)
        
        tier0head.encodeInto(bb)
  
        tails.foreach(_.encodeInto(bb))
          
        arr
      }  
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
  private[this] var refreshingForFailureOfTx: Option[UUID] = None
  
  private def loadIndexNode(tier: Int, pointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[IndexNode] = synchronized {
    cache.getNode(tier, pointer) match {  
      case Some(i) => Future.successful(i)
      
      case None => loadingNodes.get(pointer.uuid) match {
        case Some(f) => f
        
        case None =>
          val fnode = fs.system.readObject(pointer) map { dos => 
            val dnc = decodeNodeContent(dos.data)
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
  
  private[cumulofs] def destroy()(implicit ec: ExecutionContext): Future[Unit] = {
    for {
      _ <- refresh()
      oroot <- getRootIndexNode()
      _ <- oroot match {
        case None => Future.unit
        case Some(root) => root.destroy()
      }
    } yield ()
  }
  
  private[cumulofs] def getRootIndexNode()(implicit ec: ExecutionContext): Future[Option[IndexNode]] = synchronized {
    foRoot.flatMap { oroot => oroot match { 
      case None => Future.successful(None)
      case Some(root) => loadIndexNode(root.tails.length-1, root.tails.last).map(Some(_))
    }}
  }
  
  def getIndexNodeForOffset(offset: Long)(implicit ec: ExecutionContext): Future[Option[IndexNode]] = synchronized {
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
  
  def debugGetAllSegments()(implicit ec: ExecutionContext): Future[Array[Segment]] = synchronized {
    def rget(tail: IndexNode, rlist: List[Segment]): Future[List[Segment]] = {
      tail.fetchLeft() flatMap { oleft =>
        val lst = tail.segments ++: rlist
        oleft match {
          case None => Future.successful(lst)
          case Some(left) => rget(left, lst) 
        }
      }
    }
    for {
      tails <- getTails()
      rlist <- rget(tails.get(0), Nil)
    } yield rlist.toArray
  }
  
  /** Ignores duplicate refresh calls for the same transaction failure */
  def refreshOnTxFailure(tx: Transaction)(implicit ec: ExecutionContext): Unit = synchronized {
    refreshingForFailureOfTx match {
      case None =>
        refreshingForFailureOfTx = Some(tx.uuid)
        refresh()
      case Some(uuid) =>
        if (tx.uuid != uuid) {
          refreshingForFailureOfTx = Some(tx.uuid)
          refresh()
        }
    }
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
  
  def reset(onewRoot: Option[Root])(implicit ec: ExecutionContext): Unit = synchronized {
    ofTails = None
    println(s" RESETTING INDEX ROOT TO $onewRoot")
    foRoot = onewRoot match {
      case None => Future.successful(None)
      case Some(newRoot) => Future.successful(Some(newRoot))
    }
  }
  
  def getDataTail()(implicit ec: ExecutionContext): Future[Option[DataTail]] = getTails() flatMap { tails =>
    println(s"GET TAILS SUCCESSFUL ${tails.size()}")
    if (tails.size == 0)
      Future.successful(None)
    else {
      println("Getting item 0")
      val tailSegment = tails.get(0).tailSegment
      println("Reading object")
      fs.system.readObject(tailSegment.pointer) map { dos =>
        println(s"Read the object of size ${dos.size}")
        Some(DataTail(tailSegment.pointer, dos.revision, tailSegment.offset, dos.size)) 
      }
    }
  }
  
  def getTails()(implicit ec: ExecutionContext): Future[Vector[IndexNode]] = synchronized {
    println(s"getting tails with ofTails $ofTails and foRoot $foRoot")
    ofTails match {
      case Some(f) => f
      case None =>
        val ftails = foRoot flatMap { oroot => oroot match {
          case None =>
            println(s"NO root. Returning default vector")
            Future.successful(new Vector[IndexNode](2,1))
          
          case Some(root) => 
            println(s"Have root $root...")
            val fstate = Future.sequence(root.tails.map(fs.system.readObject(_)).toList)
              
            fstate map { tailStates =>
              val v = new Vector[IndexNode](root.tails.length+1, 1)
              
              tailStates.zipWithIndex.foreach { t =>
                val (dos, tier) = t
                
                val dnc = decodeNodeContent(dos.data)
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
        tailRevision: ObjectRevision, 
        tailLeft: Option[LeftPointer],
        appendedNodes: List[(Segment, Int)],
        segList: List[(Segment, Int)]): Future[(List[(Segment, Int)], IndexNode)] = {
      
      val size = encodedEntriesSize(tailLeft, segList, None)
      
      if (size <= maxNodeSize) {
        
        tx.overwrite(tail, tailRevision, encodeEntries(tailLeft, segList, None, Some(size)))
        
        val newTail = new IndexNode(tier, this, tail, tailLeft, segList.map(_._1).toArray, None, tx.txRevision, size, tx.timestamp)
        
        Future.successful((appendedNodes.reverse, newTail))
      } else {
        allocater.allocateDataObject(allocatingObject, allocatingObjectRevision, DataBuffer.Empty) flatMap { newTailPointer =>
          val remainingSegments = overwriteSplitTail(maxNodeSize, tail, tailRevision, tailLeft, segList, newTailPointer)
          val left = new LeftPointer(segList.head._1.offset, tail)
          val tailSeg = new Segment(remainingSegments.head._1.offset, newTailPointer)
          
          rappend(allocater, newTailPointer, tx.txRevision, Some(left), (tailSeg, tailSeg.encodedSize) :: appendedNodes, remainingSegments)
        }
      }
    }
    
    fs.getDataTableNodeAllocater(tier) flatMap { allocater =>
      rappend(allocater, startTail, startRevision, tails.get(tier).leftPointer, Nil, segments)
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
      refreshOnTxFailure(tx)
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
  
  def truncate(offset: Long)(implicit tx: Transaction, ec: ExecutionContext): Future[Option[Root]] = {
    
    def prep(oroot: Option[Root], orootNode: Option[IndexNode]): Future[Option[Root]] = (oroot, orootNode) match {
      case (None, _) => if (offset == 0) Future.successful(None) else Future.failed(new Exception("Holes are not yet supported"))
      
      case (Some(root), Some(rootNode)) =>
        
        for {
          (newRoot, truncatedRoot) <- rootNode.prepareTruncation(root, offset)
          _ <- DeleteTruncatedFileTask.prepare(fs, newRoot.tails.length-1, truncatedRoot)
        } yield newRoot
      
      case (Some(root), _) => if (offset == 0) Future.successful(Some(root)) else Future.failed(new Exception("Holes are not yet supported"))
      
      case _ => Future.failed(new Exception("Unsupported file state for truncation"))
    }
    for {
      _ <- refresh()
      oroot <- foRoot
      onode <- getRootIndexNode()
      newRoot <- prep(oroot, onode)
    } yield newRoot
  }
}