package com.ibm.aspen.base.btree

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import org.scalatest._
import scala.util.Success
import scala.util.Failure
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import scala.collection.immutable.SortedMap
import com.ibm.aspen.base.AspenSystem
import java.nio.ByteBuffer
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount

object BTreeUpdaterSuite {
  import BTreeSuite._
  
  case class EncodedIntKey(key: Int, encoded: ByteBuffer) extends EncodedKey {
    override def compare(that: EncodedKey) = that match {
      case t: EncodedIntKey => key - t.key
      case _ => 0
    }
  }
  
  object EncodedIntKey {
    def apply(key: Int): EncodedIntKey = {
      val bb = ByteBuffer.allocate(4)
      bb.putInt(key)
      bb.position(0)
      EncodedIntKey(key, bb)
    }
    def apply(bb: ByteBuffer): EncodedIntKey = {
      val key = bb.getInt()
      bb.position(0)
      EncodedIntKey(key, bb)
    }
  }
  
  case class Ops(
      objectPointer: ObjectPointer,
      requiredRevision: ObjectRevision,
      ops: List[BTreeOperation])
      
  object Ops {
    def apply(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, bb: ByteBuffer): Ops = {
      val ops = BTreeUpdater.decodeOperations(bb, b => EncodedIntKey(b))
      Ops(objectPointer, requiredRevision, ops)
    }
  }
  
  
  class Tx extends Transaction {
    val result = Future.failed(new Exception("Should not be used"))
    
    var appendOps: Ops = null
    var overwriteOps: Ops = null
    var invalidated: Throwable = null
    
    def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: ByteBuffer): Unit = appendOps = Ops(objectPointer, requiredRevision, data)
    def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: ByteBuffer): Unit = overwriteOps = Ops(objectPointer, requiredRevision, data)
    def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): Unit = throw new Exception("Should not be used")
    
    def invalidateTransaction(reason: Throwable): Unit = invalidated = reason
    
    def commit(): Future[Unit] = result
  }
  
  class Alloc(val tierSizeLimit: Int) extends BTreeAllocationHandler {
    
    var content: List[BTreeOperation] = Nil
    val newPtr = mkptr(1)
    
    def allocateNewNode(initialContent: ByteBuffer)(implicit t: Transaction): Future[ObjectPointer] = {
      content = BTreeUpdater.decodeOperations(initialContent.duplicate(), (bb) => EncodedIntKey(bb))
      Future.successful(newPtr)
    }
    
    def freeNode(op: ObjectPointer)(implicit t: Transaction): Unit = throw new Exception("free not used")
  }
}

class BTreeUpdaterSuite extends AsyncFunSuite with Matchers {
  import BTreeSuite.{ mkptr, awaitDuration }
  import BTreeUpdaterSuite._
  
  test("Append 1") {
    val p = mkptr(0)
    val r = ObjectRevision(0,0)
    val n = BTreeUpdater.RawBTreeNode(p, r, Nil, () => Future.failed(new Exception("Not used")))
    val alloc = new Alloc(99999)
    implicit val tx = new Tx
    val key = EncodedIntKey(1)
    val value = ByteBuffer.allocate(1)
    value.put(2.asInstanceOf[Byte])
    value.position(0)
    val ops = List(Insert(key,value))
    BTreeUpdater.prepareUpdateTransaction(n, ops, None, alloc, None) map { _ =>
      alloc.content should be (Nil)
      tx.overwriteOps should be (null)
      tx.appendOps should not be (null)
      
      val Ops(ptr, rev, decodedOps) = tx.appendOps
      
      ptr should be (p)
      rev should be (r)
      decodedOps should be (ops)
    }
  }
  
  test("Append 2") {
    val p = mkptr(0)
    val r = ObjectRevision(0,0)
    val n = BTreeUpdater.RawBTreeNode(p, r, Nil, () => Future.failed(new Exception("Not used")))
    val alloc = new Alloc(99999)
    implicit val tx = new Tx
    val key1 = EncodedIntKey(1)
    val value1 = ByteBuffer.allocate(1)
    value1.put(2.asInstanceOf[Byte])
    value1.position(0)
    val key2 = EncodedIntKey(2)
    val value2 = ByteBuffer.allocate(1)
    value1.put(3.asInstanceOf[Byte])
    value1.position(0)
    val ops = List(Insert(key1,value1), Insert(key2,value2))
    BTreeUpdater.prepareUpdateTransaction(n, ops, None, alloc, None) map { _ =>
      alloc.content should be (Nil)
      tx.overwriteOps should be (null)
      tx.appendOps should not be (null)
      
      val Ops(ptr, rev, decodedOps) = tx.appendOps
      
      ptr should be (p)
      rev should be (r)
      decodedOps should be (ops)
    }
  }
  
  
  test("Overwrite") {
    val p = mkptr(0)
    val r = ObjectRevision(0,0)
    val key1 = EncodedIntKey(1)
    val value1 = ByteBuffer.allocate(1)
    value1.put(2.asInstanceOf[Byte])
    value1.position(0)
    val n = BTreeUpdater.RawBTreeNode(p, r, List(Insert(key1,value1), Delete(key1)), () => Future.failed(new Exception("Not used")))
    val alloc = new Alloc(99999)
    implicit val tx = new Tx
    val key2 = EncodedIntKey(2)
    val value2 = ByteBuffer.allocate(1)
    value1.put(3.asInstanceOf[Byte])
    value1.position(0)
    val ops = List(Insert(key2,value2))
    BTreeUpdater.prepareUpdateTransaction(n, ops, None, alloc, Some(2)) map { _ =>
      alloc.content should be (Nil)
      tx.overwriteOps should not be (null)
      tx.appendOps should be (null)
      
      val Ops(ptr, rev, decodedOps) = tx.overwriteOps
      
      ptr should be (p)
      rev should be (r)
      decodedOps should be (ops)
    }
  }
  
  test("Split") {
    val p = mkptr(0)
    val r = ObjectRevision(0,0)
    def insert(k:Int, v:Int) = {
      val key = EncodedIntKey(k)
      val value = ByteBuffer.allocate(1)
      value.put(v.asInstanceOf[Byte])
      value.position(0)
      Insert(key,value)
    }
    val kv1 = insert(1,1)
    val kv2 = insert(2,2)
    val kv3 = insert(3,3)
    val kv4 = insert(4,4)
    val kv5 = insert(5,5)
    val kv6 = insert(6,6)
    val kv7 = insert(7,7)
    
    val n = BTreeUpdater.RawBTreeNode(p, r, List(kv5,kv1,Delete(kv1.key),kv2), () => Future.failed(new Exception("Not used")))
    val alloc = new Alloc(99999)
    implicit val tx = new Tx
    
    BTreeUpdater.prepareUpdateTransaction(n, List(kv6,kv4,kv3,kv7), None, alloc, Some(4)) map { _ =>
      alloc.content should not be (Nil)
      tx.overwriteOps should not be (null)
      tx.appendOps should be (null)
      
      val Ops(ptr, rev, decodedOps) = tx.overwriteOps
      
      ptr should be (p)
      rev should be (r)
      decodedOps should be (List(SetRightPointer(alloc.newPtr, EncodedIntKey(5)), kv2, kv3, kv4))
      alloc.content should be (List(kv5, kv6, kv7))
    }
  }
  
  test("Split fail with InsertOverflow") {
    val p = mkptr(0)
    val r = ObjectRevision(0,0)
    def insert(k:Int, v:Int) = {
      val key = EncodedIntKey(k)
      val value = ByteBuffer.allocate(1)
      value.put(v.asInstanceOf[Byte])
      value.position(0)
      Insert(key,value)
    }
    val kv1 = insert(1,1)
    val kv2 = insert(2,2)
    val kv3 = insert(3,3)
    val kv4 = insert(4,4)
    val kv5 = insert(5,5)
    val kv6 = insert(6,6)
    val kv7 = insert(7,7)
    
    val n = BTreeUpdater.RawBTreeNode(p, r, List(kv5,kv1,Delete(kv1.key),kv2), () => Future.failed(new Exception("Not used")))
    val alloc = new Alloc(99999)
    implicit val tx = new Tx
    
    recoverToSucceededIf[InsertOverflow] {
      BTreeUpdater.prepareUpdateTransaction(n, List(kv6,kv4,kv3,kv7), None, alloc, Some(1))
    }
  }
}