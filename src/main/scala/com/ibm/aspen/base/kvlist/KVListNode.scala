package com.ibm.aspen.base.kvlist

import com.ibm.aspen.core.objects.ObjectRevision
import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.core.objects.ObjectState
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.kvlist.KVListCodec.KVListOperation
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.DataObjectState

class KVListNode(
    val list: KVList,
    val nodePointer: KVListNodePointer,
    val nodeRevision: ObjectRevision,
    val rightNode: Option[KVListNodePointer],  // Guaranteed to be accurate for the corresponding nodeRevision
    val contentLength: Int, // Current number of bytes stored in the node
    val content: SortedMap[Array[Byte],Array[Byte]]) {
  
  import KVListNode._
  
  def minimum = nodePointer.minimum
  
  def refresh()(implicit ec: ExecutionContext) : Future[KVListNode] = list.refreshNode(this) 
  
  def fetchRightNode()(implicit ec: ExecutionContext): Future[Option[KVListNode]] = rightNode match {
    case None => Future.successful(None)
    case Some(rptr) => list.fetchNode(rptr) map (Some(_))
  }
  
  def keyWithinRange(key: Array[Byte]): Boolean = {
    val rok = rightNode match { 
      case None => true
      case Some(next) => list.compareKeys(key, next.minimum) < 0
    }
    
    if (minimum.length > 0)
      list.compareKeys(key, nodePointer.minimum) >= 0 && rok
    else
      rok
  }
  
  /** Scans to the right until the node that owns the range is found */
  def fetchContainingNode(key: Array[Byte])(implicit ec: ExecutionContext): Future[KVListNode] = scanToWithinRange(this, key, None)
  
  /** Scans to the right until the node that owns the range is found or the next node is blacklisted */
  def fetchContainingNode(key: Array[Byte], blacklisted: Set[KVListNodePointer])(implicit ec: ExecutionContext): Future[KVListNode] = scanToWithinRange(this, key, Some(blacklisted))
  
  /**
   * split(allocatingTransaction, ExecutionContext, OriginalNode, UpdatedNode, NewAllocatedNode) 
   */
  def update(inserts: List[(Array[Byte], Array[Byte])], deletes: List[Array[Byte]], onSplit: (Transaction, ExecutionContext, KVListNode, KVListNode, KVListNode) => Unit)
            (implicit transaction: Transaction, ec: ExecutionContext): Future[Future[(KVListNode, Option[KVListNode])]] = {
    val promise = Promise[Future[(KVListNode, Option[KVListNode])]]()
    
    // Update cache on successful modification
    promise.future foreach {
      f => f foreach {
        t =>
          val (updatedNode, splitNode) = t
          list.updateCachedNode(updatedNode)
          splitNode.foreach(list.updateCachedNode)
      }
    }
    
    def failTransaction(reason: Throwable) = {
      transaction.invalidateTransaction(reason)
      promise.failure(reason)
    }
    
    try {
      // Build a list of all operations and check key ranges
      val a = List[KVListCodec.KVListOperation]()
      val b = inserts.foldLeft(a)((l, i) => {
        if (!keyWithinRange(i._1)) throw new KeyOutOfRange
        KVListCodec.Insert(i._1, i._2) :: l
      })
      val opsList = deletes.foldLeft(b)((l, d) => {
        if (!keyWithinRange(d)) throw new KeyOutOfRange
        KVListCodec.Delete(d) :: l
      })
      
      // Apply insert and delete operations to 'content'
      val compactedContent = deletes.foldLeft(inserts.foldLeft(content)((m, t) => m + t))((m, d) => m - d)
      
      val sizeLimit = list.objectAllocater.nodeSizeLimit
      val nodeSizeLimit = java.lang.Math.min(sizeLimit, nodePointer.objectPointer.size.getOrElse(sizeLimit))
      //
      // Try Append
      //
      val (appendOps, appendSize, appendOpCount) = KVListCodec.encodeOperations(opsList)
        
      if (appendSize <= nodeSizeLimit - contentLength) {
          
          val newRevision = transaction.append(nodePointer.objectPointer, nodeRevision, KVListCodec.opsToDataBuffer(appendOps, appendSize))
          val updatedNode = new KVListNode(list, nodePointer, newRevision, rightNode, contentLength+appendSize, compactedContent)
          
          promise.success(transaction.result.map(_ => (updatedNode, None)))
      } else {
        //
        // Try Overwrite
        //
        val insertOps = compactedContent.iterator.map(t => KVListCodec.Insert(t._1, t._2)).toList
        
        val fullOps = rightNode match {
          case None => insertOps
          case Some(rp) => KVListCodec.SetRightPointer(rp.objectPointer, rp.minimum) :: insertOps
        }
        
        val (overwriteOps, overwriteSize, overwriteOpCount) = KVListCodec.encodeOperations(fullOps)
        
        implicit val keyOrdering = new KVListCodec.KeyOrdering(list.compareKeys)
        
        if (overwriteSize <= sizeLimit) {
          
          val buf = KVListCodec.opsToDataBuffer(overwriteOps, overwriteSize)
          
          assert(buf.size == overwriteSize)
          
          val newContent = insertOps.foldLeft(SortedMap[Array[Byte], Array[Byte]]())((m, ins) => m + (ins.key -> ins.value))
          val newRevision = transaction.overwrite(nodePointer.objectPointer, nodeRevision, buf)
          val updatedNode = new KVListNode(list, nodePointer, newRevision, rightNode, overwriteSize, newContent)
          
          promise.success(transaction.result.map(_ => (updatedNode, None)))
        } else {
          //
          // Try Split
          //
          val insertOpsCount = rightNode match {
            case None => overwriteOpCount
            case Some(_) => overwriteOpCount - 1
          }
          
          val (leftInsertOps, rightInsertOps) = insertOps.splitAt(insertOpsCount/2)
    
          val (rightEncoded, rightSize, _) = {
            val lst = rightNode match {
              case None => rightInsertOps
              case Some(rp) => KVListCodec.SetRightPointer(rp.objectPointer, rp.minimum) :: rightInsertOps
            }
            KVListCodec.encodeOperations(lst)
          }

          val rightData = KVListCodec.opsToDataBuffer(rightEncoded, rightSize)
          
          if (rightData.size > nodeSizeLimit)
            throw new InsertOverflow
         
          list.objectAllocater.allocate(nodePointer.objectPointer, nodeRevision, rightData, transaction.timestamp()) map { newNodePointer => 
            val (leftEncoded, leftSize, _) = KVListCodec.encodeOperations(KVListCodec.SetRightPointer(newNodePointer, rightInsertOps.head.key) :: leftInsertOps)

            val leftData = KVListCodec.opsToDataBuffer(leftEncoded, leftSize)
            
            assert(leftData.size == leftSize)
            
            if (leftSize > sizeLimit) {
              val err = new InsertOverflow
              failTransaction(err)
              throw err
            }
            
            val newContent = leftInsertOps.foldLeft(SortedMap[Array[Byte], Array[Byte]]())((m, ins) => m + (ins.key -> ins.value))
            val newRightNodePointer = KVListNodePointer(newNodePointer, rightInsertOps.head.key)
            val updatedNode = new KVListNode(list, nodePointer, transaction.txRevision, Some(newRightNodePointer), leftSize, newContent)
            
            val newRightContent = rightInsertOps.foldLeft(SortedMap[Array[Byte], Array[Byte]]())((m, ins) => m + (ins.key -> ins.value))
            val newRightNode = new KVListNode(list, newRightNodePointer, transaction.txRevision, rightNode, rightSize, newRightContent)
            
            try {
              onSplit(transaction, ec, this, updatedNode, newRightNode)
            } catch {
              case t: Throwable => 
                failTransaction(t)
                throw t
            }
            
            transaction.overwrite(nodePointer.objectPointer, nodeRevision, leftData)
            promise.success(transaction.result.map(_ => (updatedNode, Some(newRightNode))))
          }
        }
      }
      
    } catch {
      case reason: Throwable => failTransaction(reason)
    }
    
    promise.future
  }
  
}

object KVListNode {
  
  def apply(list: KVList, nodePointer: KVListNodePointer, osd: DataObjectState): KVListNode = {
    val (content, rightPointer) = KVListCodec.decodeNodeContent(list.compareKeys, osd.data)
    
    new KVListNode(list, nodePointer, osd.revision, rightPointer, osd.data.size, content)
  }
  
  def scanToWithinRange(sourceNode: KVListNode, key: Array[Byte], blacklisted: Option[Set[KVListNodePointer]])(implicit ec: ExecutionContext): Future[KVListNode] = {
    val p = Promise[KVListNode]()
    
    def scanRight(node: KVListNode, key: Array[Byte])(implicit ec: ExecutionContext): Unit = {
      if (node.keyWithinRange(key))
        p.success(node)
      else {
        val nextIsBlacklisted = blacklisted match {
          case None => false
          case Some(blacklist) => node.rightNode match {
            case None => false
            case Some(rn) => blacklist.contains(rn)
          }
        }
        
        if (nextIsBlacklisted)
          p.success(node) // stop here
        else {
          node.fetchRightNode() onComplete {
            case Failure(err) => p.failure(err)
            case Success(onode) => onode match {
              case None => p.success(node)
              case Some(rnode) => scanRight(rnode, key)
            }
          }
        }
      }
    }
    
    if (sourceNode.minimum.length != 0 && sourceNode.list.compareKeys(key, sourceNode.minimum) < 0)
      p.failure(new KeyOutOfRange)
    else 
      scanRight(sourceNode, key)  
    
    p.future
  }  
}