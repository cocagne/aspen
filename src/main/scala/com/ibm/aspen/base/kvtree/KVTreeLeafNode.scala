package com.ibm.aspen.base.kvtree

import java.nio.ByteBuffer
import scala.collection.immutable.SortedMap
import com.ibm.aspen.core.objects.ObjectRevision
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.ibm.aspen.base.ObjectStateAndData
import com.ibm.aspen.base.Transaction
import scala.concurrent.Promise

class KVTreeLeafNode(
    val tree: KVTreeImpl,
    val nodePointer: KVTreeNodePointer,
    val nodeRevision: ObjectRevision,
    val leftNode: Option[KVTreeNodePointer],
    val rightNode: Option[KVTreeNodePointer],
    val content: SortedMap[Array[Byte],Array[Byte]]) extends KVTreeNode {
  
  import KVTreeCodec.{Insert, Delete}
  
  val tier = 0
  
  override def fetchRightNode()(implicit ec: ExecutionContext): Future[Option[KVTreeLeafNode]] = rightNode match {
    case None => Future.successful(None)
    case Some(rptr) => tree.system.readObject(rptr.objectPointer, None) map {
      osd => Some(KVTreeLeafNode(tree, rptr, Some(this.nodePointer), osd))
    }
  }
  
  def update(ops: List[Either[Delete,Insert]])(implicit transaction: Transaction, ec: ExecutionContext): Future[Future[(KVTreeLeafNode, Option[KVTreeLeafNode])]] = {
    val promise = Promise[Future[(KVTreeLeafNode, Option[KVTreeLeafNode])]]()
    
    def failTransaction(reason: Throwable) = {
      transaction.invalidateTransaction(reason)
      promise.failure(reason)
    }
    
    try {
      ops foreach { op => op match {
        case Left(del) => if (!keyWithinRange(del.key)) throw new KeyOutOfRange
        case Right(ins) => if (!keyWithinRange(ins.key)) throw new KeyOutOfRange
      }}
      
      val sizeLimit = tree.tierSizelimit(0)
      val nodeSizeLimit = java.lang.Math.min(sizeLimit, nodePointer.objectPointer.size.getOrElse(sizeLimit))
      
      val opsList: List[KVTreeCodec.KVTreeOperation] = ops.map{eop => eop match {
        case Left(op) => op
        case Right(op) => op
      }}
      
      // Try Append
      val (appendOps, appendSize, appendOpCount) = KVTreeCodec.encodeOperations(opsList)
        
      if (appendSize <= nodeSizeLimit - nodeRevision.currentSize) {
          transaction.append(nodePointer.objectPointer, nodeRevision, KVTreeCodec.opsToByteBuffer(appendOps, appendSize))
          val newRevision = nodeRevision.copy(currentSize = nodeRevision.currentSize+appendSize)
          val newContent = ops.foldLeft(content)((m, op) => op match {
            case Left(del) => m - del.key
            case Right(ins) => m + (ins.key -> ins.value)
          })
          val updatedNode = new KVTreeLeafNode(tree, nodePointer, newRevision, leftNode, rightNode, newContent)
          
          promise.success(transaction.result.map(_ => (updatedNode, None)))
      } else {
        // Try Overwrite
        val compactedContent = ops.foldLeft(content)((m, eop) => eop match {
          case Left(del) => m - del.key
          case Right(ins) => m + (ins.key -> ins.value)
        })
        
        val insertOps = compactedContent.iterator.map(t => KVTreeCodec.Insert(t._1, t._2)).toList
        
        val fullOps = rightNode match {
          case None => insertOps
          case Some(rp) => KVTreeCodec.SetRightPointer(rp.objectPointer, rp.minimum) :: insertOps
        }
        
        val (overwriteOps, overwriteSize, overwriteOpCount) = KVTreeCodec.encodeOperations(fullOps)
        
        implicit val keyOrdering = new KVTreeCodec.KeyOrdering(tree.compareKeys)
        
        if (overwriteSize <= sizeLimit) {
          val buf = KVTreeCodec.opsToByteBuffer(overwriteOps, overwriteSize)
          
          assert(buf.limit - buf.position == sizeLimit)
          
          transaction.overwrite(nodePointer.objectPointer, nodeRevision, buf)
          
          val newContent = insertOps.foldLeft(SortedMap[Array[Byte], Array[Byte]]())((m, ins) => m + (ins.key -> ins.value))
          val newRevision = ObjectRevision(nodeRevision.overwriteCount+1, overwriteSize)
          val updatedNode = new KVTreeLeafNode(tree, nodePointer, newRevision, leftNode, rightNode, newContent)
          
          promise.success(transaction.result.map(_ => (updatedNode, None)))
        } else {
          // Try Split
          
          val insertOpsCount = rightNode match {
            case None => overwriteOpCount
            case Some(_) => overwriteOpCount - 1
          }
          
          val (leftInsertOps, rightInsertOps) = insertOps.splitAt(insertOpsCount/2)
    
          val (rightEncoded, rightSize, _) = {
            val lst = rightNode match {
              case None => rightInsertOps
              case Some(rp) => KVTreeCodec.SetRightPointer(rp.objectPointer, rp.minimum) :: rightInsertOps
            }
            KVTreeCodec.encodeOperations(lst)
          }

          val rightData = KVTreeCodec.opsToByteBuffer(rightEncoded, rightSize)
          
          if (rightData.limit - rightData.position > nodeSizeLimit)
            throw new InsertOverflow
         
          tree.objectAllocater.allocate(nodePointer.objectPointer, nodeRevision, tier, rightData) map { newNodePointer => 
            val (leftEncoded, leftSize, _) = KVTreeCodec.encodeOperations(KVTreeCodec.SetRightPointer(newNodePointer, rightInsertOps.head.key) :: leftInsertOps)

            val leftData = KVTreeCodec.opsToByteBuffer(leftEncoded, leftSize)
            
            assert(leftData.limit - leftData.position == leftSize)
            
            if (leftSize > sizeLimit) {
              val err = new InsertOverflow
              failTransaction(err)
              throw err
            }
            
            val newContent = leftInsertOps.foldLeft(SortedMap[Array[Byte], Array[Byte]]())((m, ins) => m + (ins.key -> ins.value))
            val newRevision = ObjectRevision(nodeRevision.overwriteCount+1, leftSize)
            val newRightNodePointer = KVTreeNodePointer(newNodePointer, rightInsertOps.head.key)
            val updatedNode = new KVTreeLeafNode(tree, nodePointer, newRevision, leftNode, Some(newRightNodePointer), newContent)
            
            val newRightContent = rightInsertOps.foldLeft(SortedMap[Array[Byte], Array[Byte]]())((m, ins) => m + (ins.key -> ins.value))
            val newRightRevision = ObjectRevision(0, rightSize)
            val newRightNode = new KVTreeLeafNode(tree, newRightNodePointer, newRightRevision, Some(nodePointer), rightNode, newRightContent)
            
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

object KVTreeLeafNode {
  
  
  def apply(
      tree: KVTreeImpl,
      nodePointer: KVTreeNodePointer,
      nodeRevision: ObjectRevision,
      leftNode: Option[KVTreeNodePointer],
      rightNode: Option[KVTreeNodePointer],
      kvpairs: List[(Array[Byte], Array[Byte])]): KVTreeLeafNode = {
    
    implicit val ordering = new KVTreeCodec.KeyOrdering(tree.compareKeys)
    
    val content = kvpairs.foldLeft(SortedMap[Array[Byte],Array[Byte]]())((m, t) => m + (t._1 -> t._2))
    
    new KVTreeLeafNode(tree, nodePointer, nodeRevision, leftNode, rightNode, content)
  }
  
  def apply(
      tree: KVTreeImpl,
      nodePointer: KVTreeNodePointer,
      leftNode: Option[KVTreeNodePointer],
      osd: ObjectStateAndData): KVTreeLeafNode = {
    
    implicit val ordering = new KVTreeCodec.KeyOrdering(tree.compareKeys)
    
    var content = SortedMap[Array[Byte],Array[Byte]]()
    var rightPointer: Option[KVTreeNodePointer] = None
    
    KVTreeCodec.decodeOperations(osd.data).foreach { op => op match {
      case ins: KVTreeCodec.Insert => content += (ins.key -> ins.value)
      case del: KVTreeCodec.Delete => content -= del.key
      case srp: KVTreeCodec.SetRightPointer => rightPointer = Some(KVTreeNodePointer(srp.op, srp.minimum))
    }}
    
    new KVTreeLeafNode(tree, nodePointer, osd.revision, leftNode, rightPointer, content)
  }
}