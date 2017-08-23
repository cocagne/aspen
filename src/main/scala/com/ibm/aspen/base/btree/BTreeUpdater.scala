package com.ibm.aspen.base.btree

import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.ObjectState
import com.ibm.aspen.core.network.{Codec => NetworkCodec}
import java.nio.ByteBuffer
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.ObjectRevision
import scala.annotation.tailrec
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext
import scala.collection.SortedMap

object BTreeUpdater {
   
  sealed abstract class OpCode {
    def opCode: Byte
    def integerSize: Int
    def encodedSize: Int
    def encodeSize(bb: ByteBuffer, size: Long): Unit
    def decodeSize(bb: ByteBuffer): Long
    def encode(bb: ByteBuffer): Unit
  }
  
  trait Size1 {
    def integerSize = 1
    def encodeSize(bb: ByteBuffer, size: Long): Unit = bb.put(size.asInstanceOf[Byte])
    def decodeSize(bb: ByteBuffer): Long = bb.get()
  }
  trait Size2 {
    def integerSize = 2
    def encodeSize(bb: ByteBuffer, size: Long): Unit = bb.putShort(size.asInstanceOf[Short])
    def decodeSize(bb: ByteBuffer): Long = bb.getShort()
  }
  trait Size4 {
    def integerSize = 4
    def encodeSize(bb: ByteBuffer, size: Long): Unit = bb.putInt(size.asInstanceOf[Int])
    def decodeSize(bb: ByteBuffer): Long = bb.getInt()
  }
  
  abstract class KeyedOp extends OpCode {
    def key: EncodedKey
  }
  
  abstract class InsertOp extends KeyedOp {
    def value: ByteBuffer
    def encodedSize: Int = 1 + integerSize * 2 + key.encoded.capacity() + value.capacity()
    def encode(bb: ByteBuffer) = {
      bb.put(opCode)
      encodeSize(bb, key.encoded.capacity())
      encodeSize(bb, value.capacity())
      key.encoded.position(0)
      value.position(0)
      bb.put(key.encoded)
      bb.put(value)
      key.encoded.position(0)
      value.position(0)
    }
  }
  abstract class DeleteOp extends KeyedOp {
    def encodedSize: Int = 1 + integerSize + key.encoded.capacity()
    def encode(bb: ByteBuffer) = {
      bb.put(opCode)
      encodeSize(bb, key.encoded.capacity())
      key.encoded.position(0)
      bb.put(key.encoded)
      key.encoded.position(0)
    }
  }
  abstract class SetRightPointerOp extends OpCode {
    def pointer: ByteBuffer
    def minimum: EncodedKey
    def encodedSize: Int = 1 + integerSize * 2 + pointer.capacity() + minimum.encoded.capacity()
    def encode(bb: ByteBuffer) = {
      bb.put(opCode)
      encodeSize(bb, pointer.capacity())
      encodeSize(bb, minimum.encoded.capacity())
      pointer.position(0)
      minimum.encoded.position(0)
      bb.put(pointer)
      bb.put(minimum.encoded)
      pointer.position(0)
      minimum.encoded.position(0)
    }
  }
  
  case class Insert1(key: EncodedKey, value: ByteBuffer) extends InsertOp with Size1 {
    val opCode:Byte = 0
  }
  case class Insert2(key: EncodedKey, value: ByteBuffer) extends InsertOp with Size2 {
    val opCode:Byte = 1
  }
  case class Insert4(key: EncodedKey, value: ByteBuffer) extends InsertOp with Size4 {
    val opCode:Byte = 2
  }
  case class Delete1(key: EncodedKey) extends DeleteOp with Size1 {
    val opCode:Byte = 3
  }
  case class Delete2(key: EncodedKey) extends DeleteOp with Size2 {
    val opCode:Byte = 4
  }
  case class Delete4(key: EncodedKey) extends DeleteOp with Size4 {
    val opCode:Byte = 5
  }
  case class SetRightPointer1(pointer: ByteBuffer, minimum: EncodedKey) extends SetRightPointerOp with Size1 {
    val opCode:Byte = 6
  }
  case class SetRightPointer2(pointer: ByteBuffer, minimum: EncodedKey) extends SetRightPointerOp with Size2 {
    val opCode:Byte = 7
  }
  case class SetRightPointer4(pointer: ByteBuffer, minimum: EncodedKey) extends SetRightPointerOp with Size4 {
    val opCode:Byte = 8
  }
  
  def decodeOperations(bb: ByteBuffer, bbToEncodedKey: (ByteBuffer) => EncodedKey): List[BTreeOperation] = {
    var ops = List[BTreeOperation]()
    
    while (bb.position() < bb.capacity()) 
      ops = decodeOp(bb, bbToEncodedKey) :: ops
    
    ops.reverse
  }
  
  def decodeOp(bb: ByteBuffer, bbToEncodedKey: (ByteBuffer) => EncodedKey): BTreeOperation = {
    def s1() = bb.get()
    def s2() = bb.getShort()
    def s4() = bb.getInt()
    
    def getInsert(sizeFn: () => Int): Insert = {
      val keyArray = new Array[Byte](sizeFn())
      val valueArray = new Array[Byte](sizeFn())
      bb.get(keyArray)
      bb.get(valueArray)
      Insert(bbToEncodedKey(ByteBuffer.wrap(keyArray)), ByteBuffer.wrap(valueArray))
    }
    
    def getDelete(sizeFn: () => Int): Delete = {
      val keyArray = new Array[Byte](sizeFn())
      bb.get(keyArray)
      Delete(bbToEncodedKey(ByteBuffer.wrap(keyArray)))
    }
    
    def getSetPointer(sizeFn: () => Int): SetRightPointer = {
      val ptrArray = new Array[Byte](sizeFn())
      val minimumArray = new Array[Byte](sizeFn())
      bb.get(ptrArray)
      bb.get(minimumArray)
      SetRightPointer(NetworkCodec.byteBufferToObjectPointer(ByteBuffer.wrap(ptrArray)), bbToEncodedKey(ByteBuffer.wrap(minimumArray)))
    }
    
    bb.get() match {
      case 0 => getInsert(s1 _)
      case 1 => getInsert(s2 _)
      case 2 => getInsert(s4 _)
      case 3 => getDelete(s1 _)
      case 4 => getDelete(s2 _)
      case 5 => getDelete(s4 _)
      case 6 => getSetPointer(s1 _)
      case 7 => getSetPointer(s2 _)
      case 8 => getSetPointer(s4 _)
    }
  }
  
  def bytesNeeded(buf: ByteBuffer): Int = if (buf.capacity() <= Byte.MaxValue) 
   1 else if (buf.capacity() <= Short.MaxValue)
   2 else if (buf.capacity() <= Int.MaxValue)
   4 else
   8
   
  def bytesNeeded(key: EncodedKey): Int = bytesNeeded(key.encoded)
   
  def mkInsert(key: EncodedKey, value: ByteBuffer): InsertOp = java.lang.Math.max(bytesNeeded(key), bytesNeeded(value)) match {
     case 1 => Insert1(key, value)
     case 2 => Insert2(key, value)
     case 4 => Insert4(key, value)
     case _ => throw new EncodingSizeError
   }
  
  def mkDelete(key: EncodedKey): DeleteOp = bytesNeeded(key) match {
     case 1 => Delete1(key)
     case 2 => Delete2(key)
     case 4 => Delete4(key)
     case _ => throw new EncodingSizeError
   }
  
  def mkSetRight(pointer: ByteBuffer, minimum: EncodedKey): SetRightPointerOp = {
    java.lang.Math.max(bytesNeeded(pointer), bytesNeeded(minimum)) match {
     case 1 => SetRightPointer1(pointer, minimum)
     case 2 => SetRightPointer2(pointer, minimum)
     case 4 => SetRightPointer4(pointer, minimum)
     case _ => throw new EncodingSizeError
   }
  }
  
  def mkSetRight(pointer:ObjectPointer, minimum:EncodedKey): SetRightPointerOp = mkSetRight(NetworkCodec.objectPointerToByteBuffer(pointer), minimum)
  
  /** Returns (size, opcount) */
  def encodedOperationsSize(encoded:List[OpCode]): (Int,Int) = encoded.foldLeft((0,0))((t, op) => (t._1 + op.encodedSize, t._2 + 1))
  
  def encodeOperations(operations: List[BTreeOperation]): List[OpCode] = operations.map(op => (op: @unchecked) match {
    case o: Insert => mkInsert(o.key, o.value)
    case o: Delete => mkDelete(o.key)
    case o: SetRightPointer => mkSetRight(NetworkCodec.objectPointerToByteBuffer(o.op), o.minimum)
  })
  
  case class RawBTreeNode(
      pointer: ObjectPointer,
      revision: ObjectRevision,
      content: List[BTreeOperation],
      fetchLeftNode: () => Future[Option[RawBTreeNode]])
  
  /** Returns a sorted list of insert operations */
  def compactToInsertOps(currentOps: List[BTreeOperation], newOps: List[BTreeContentOperation]): List[InsertOp] = {
    val opIter = currentOps.iterator ++ newOps.iterator 
    
    val content = opIter.foldLeft(SortedMap[EncodedKey, Insert]())( (m, op) => op match {
      case o: Insert => m + (o.key -> o)
      case d: Delete => m - d.key
      case t => m // ignore SetRightPointer. That's explicitly handled elsewhere
    })
    
    content.valuesIterator.map(ins => mkInsert(ins.key, ins.value)).toList
  }
  
  def opsToByteBuffer(ops: List[OpCode], encodedSize: Int): ByteBuffer = {
    val bb = ByteBuffer.allocate(encodedSize)
    ops.foreach(_.encode(bb))
    bb.position(0)
    bb
  }
  
  def split(node: RawBTreeNode, 
            insertOps: List[InsertOp],
            insertOpsSize: Int,
            insertOpsCount: Int,
            rightPointerOp: Option[SetRightPointerOp], 
            nodeSizeLimit: Int, 
            operationCountLimit: Option[Int], 
            allocationHandler: BTreeAllocationHandler)(implicit transaction: Transaction, ec: ExecutionContext): Future[Unit] = {
    
    val (leftInsertOps, rightInsertOps) = insertOps.splitAt(insertOpsCount/2)
    
    val rightOps = rightPointerOp match {
      case None => rightInsertOps
      case Some(rp) => rp :: rightInsertOps
    }
    val rightData = opsToByteBuffer(rightOps, encodedOperationsSize(rightOps)._1)
    
    val opCountExceeded = operationCountLimit match {
      case None => false
      case Some(limit) => (insertOpsCount/2 + 1) > limit
    }
    
    if (rightData.capacity() > nodeSizeLimit || opCountExceeded)
      return Future.failed(new InsertOverflow)
   
    allocationHandler.allocateNewNode(rightData) map { newNodePointer => 
      val leftOps = mkSetRight(newNodePointer, rightInsertOps.head.key) :: leftInsertOps
      val leftData = opsToByteBuffer(leftOps, encodedOperationsSize(leftOps)._1)
      
      if (leftData.capacity() > allocationHandler.tierSizeLimit)
        throw new InsertOverflow
      
      transaction.overwrite(node.pointer, node.revision, leftData)
    }
  }
      
  /* Returns a future to when the implicit transaction is ready for commit
   * 
   */
  def prepareUpdateTransaction(
      node: RawBTreeNode,
      operations: List[BTreeContentOperation],
      nodeSizeLimit: Option[Int],
      allocationHandler: BTreeAllocationHandler,
      operationCountLimit: Option[Int])(implicit transaction: Transaction, ec: ExecutionContext): Future[Unit] = {
    
    val promise = Promise[Unit]()
    
    def failTransaction(reason: Throwable) = {
      transaction.invalidateTransaction(reason)
      promise.failure(reason)
    }
    
    // Find the current right pointer and verify that all items are legal for insertion
    def verify(): Option[SetRightPointer] = {
      
      @tailrec
      def findCurrentRightPointer(ops: List[BTreeOperation], rp: Option[SetRightPointer]): Option[SetRightPointer] = if (ops.isEmpty) {
        rp
      } else {
        ops.head match {
          case srp: SetRightPointer => findCurrentRightPointer(ops.tail, Some(srp))
          case _ => findCurrentRightPointer(ops.tail, rp)
        }
      }
      
      val rpointer = findCurrentRightPointer(node.content, None)
      
      rpointer foreach { rp => 
        operations.foreach { op => 
          op match {
            case o: Insert => if (o.key >= rp.minimum) throw new KeyOutOfRange
            case o: Delete => if (o.key >= rp.minimum) throw new KeyOutOfRange
            case _ =>
          }
        }
      }
      
      rpointer
    }
    
    try {
      val currentRightPointer = verify()
    
      val sizeLimit = nodeSizeLimit.getOrElse( allocationHandler.tierSizeLimit )
      
      def tryAppend(): Boolean = {
        val appendOps = encodeOperations(operations)
        val (size, count) = encodedOperationsSize(appendOps) 
        
        val countOkay = operationCountLimit match {
          case None => true
          case Some(limit) => (node.content.length + count) <= limit
        }
        
        if ( size <= sizeLimit - node.revision.currentSize && countOkay ) {
          transaction.append(node.pointer, node.revision, opsToByteBuffer(appendOps, size))  
          true
        } else
          false
      }
      
      if (tryAppend()) {
        promise.success(())
      } else {
        
        val insertOps = compactToInsertOps(node.content, operations)
        val rightPointerOp = currentRightPointer.map(srp => mkSetRight(NetworkCodec.objectPointerToByteBuffer(srp.op), srp.minimum))
        val (insertSize, insertCount) = encodedOperationsSize(insertOps)
        
        if (insertSize > sizeLimit)
          throw new InsertOverflow
        
        val (overwriteOps, overwriteSize, overwriteCount) = rightPointerOp match {
          case None => (insertOps, insertSize, insertCount)
          case Some(rp) => (rp :: insertOps, insertSize + encodedOperationsSize(rp :: Nil)._1, insertCount + 1)
        }
        
        val overwriteCountOkay = operationCountLimit match {
          case None => true
          case Some(limit) => overwriteCount <= limit
        }
        
        if (overwriteSize < sizeLimit && overwriteCountOkay) {
          // TODO: detect Join threshold
          transaction.overwrite(node.pointer, node.revision, opsToByteBuffer(overwriteOps, overwriteSize))
          promise.success(())
        } else {
          split(node, insertOps, insertSize, insertCount, rightPointerOp, sizeLimit, operationCountLimit, allocationHandler) onComplete {
            case Success(_) => promise.success(())
            case Failure(reason) => failTransaction(reason)
          }
        }
      }
    } catch {
      case reason: Throwable => failTransaction(reason)
    }
    
    promise.future 
  }
}