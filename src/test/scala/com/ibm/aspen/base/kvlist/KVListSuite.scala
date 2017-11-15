package com.ibm.aspen.base.kvlist

import scala.concurrent._
import scala.concurrent.duration._
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
import com.ibm.aspen.base.ObjectStateAndData
import com.ibm.aspen.core.DataBuffer

object KVListSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
  val poolUUID = new UUID(0,0)
  
  def mkptr(objectNum:Int) = ObjectPointer(new UUID(0,objectNum), poolUUID, None, Replication(3,2), new Array[StorePointer](0)) 
  
  def compareKeys(a: Array[Byte], b: Array[Byte]): Int = {
    val ia = ByteBuffer.wrap(a).getInt()
    val ib = ByteBuffer.wrap(b).getInt()
    ia - ib
  }
  
  def toKVset(a:SortedMap[Array[Byte], Array[Byte]]): Set[(Int,Int)] = {
    def dc(b: Array[Byte]): Int = ByteBuffer.wrap(b).getInt
    a.iterator.map(t => (dc(t._1), dc(t._2))).toSet
  }
  
  implicit val keyOrdering = new KVListCodec.KeyOrdering(compareKeys)
  
  class Tx extends Transaction {
    val uuid = new UUID(0,0)
    
    val p = Promise[Unit]()
    
    val result = p.future
    
    case class Op(pointer: ObjectPointer, revision: ObjectRevision, content: SortedMap[Array[Byte], Array[Byte]], rightPointer: Option[KVListNodePointer])
    
    var appendOp: Op = null
    var overwriteOp: Op = null
    var invalidated: Throwable = null
    
    override def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = {  
      val (rightContent, rightRightPointer) = KVListCodec.decodeNodeContent(compareKeys, data)
      appendOp = Op(objectPointer, requiredRevision, rightContent, rightRightPointer)
      requiredRevision.append(data.size)
    }
    
    override def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision =  {
      val (rightContent, rightRightPointer) = KVListCodec.decodeNodeContent(compareKeys, data)
      overwriteOp = Op(objectPointer, requiredRevision, rightContent, rightRightPointer)
      requiredRevision.overwrite(data.size)
    }
    
    override def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount = throw new Exception("Should not be used")
    
    def invalidateTransaction(reason: Throwable): Unit = invalidated = reason
    
    def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit = ()
    
    def commit()(implicit ec: ExecutionContext): Future[Unit] = {
      p.success(())
      p.future
    }
  }
  
  class TList(nodeSizeLimitX: Int) extends KVList {
    
    var m = Map[UUID, ObjectStateAndData]()
    
    // case class ObjectStateAndData(pointer: ObjectPointer, revision:ObjectRevision, refcount:ObjectRefcount, data: ByteBuffer)
    def fetchNodeObject(objectPointer: ObjectPointer): Future[ObjectStateAndData] = Future.successful(m(objectPointer.uuid))
    
    def compareKeys(a: Array[Byte], b: Array[Byte]): Int = KVListSuite.compareKeys(a,b)
    
    class Alloc extends KVListNodeAllocater {
      var nextAllocId = 1
      
      val allocationPolicyUUID: UUID = new UUID(100,100)
      
      def allocate(
          targetObject:ObjectPointer, targetRevision: ObjectRevision, 
          initialContent: DataBuffer)(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer] = {
        val p = mkptr(nextAllocId)
        nextAllocId += 1
        
        m += (p.uuid -> ObjectStateAndData(p, ObjectRevision(0,initialContent.size), ObjectRefcount(0,1), initialContent))
        
        Future.successful(p)
      }
      
      val nodeSizeLimit: Int = nodeSizeLimitX
    }
    
    val rootObjectPointer: ObjectPointer = mkptr(0)
    
    val objectAllocater: KVListNodeAllocater = new Alloc
    
    m += (rootObjectPointer.uuid -> ObjectStateAndData(rootObjectPointer, ObjectRevision(0,0), ObjectRefcount(0,1), DataBuffer(ByteBuffer.allocate(0))))
    
    def root = KVListNode(this, KVListNodePointer(rootObjectPointer, new Array[Byte](0)), m(rootObjectPointer.uuid))
  }
  
  def k(key: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(key).array()
  def kv(key: Int, value: Int): (Array[Byte], Array[Byte]) = {
    (ByteBuffer.allocate(4).putInt(key).array(),  ByteBuffer.allocate(4).putInt(value).array())
  }
  
  def noSplit(t:Transaction, ec:ExecutionContext, orig:KVListNode, updated:KVListNode, newNode:KVListNode): Unit = throw new Exception("Should not be called")
}

class KVListSuite extends AsyncFunSuite with Matchers {
  import KVListSuite._
  
  test("Insert") {
    implicit val t = new Tx
    val l = new TList(999)
    val orig = l.root
    
    orig.content.isEmpty should be (true)
    
    val x = kv(1,1)
    
    val expectedContent = SortedMap(x)
    
    orig.update(List(x), Nil, noSplit) flatMap { result =>
      t.commit()
    
      result map { r => 
          r._1.content should be (expectedContent)
          r._2 should be (None)
      }
    }
  }
  
  test("Multi Insert") {
    implicit val t = new Tx
    val l = new TList(999)
    val orig = l.root
    
    orig.content.isEmpty should be (true)
    
    val x1 = kv(1,1)
    val x2 = kv(2,2)
    
    val expectedContent = SortedMap(x1, x2)
    
    orig.update(List(x2, x1), Nil, noSplit) flatMap { result =>
      t.commit()
    
      result map { r => 
          r._1.content should be (expectedContent)
          r._2 should be (None)
      }
    }
  }
  
  test("Insert & Delete ") {
    val l = new TList(999)
    
    val x1 = kv(1,1)
    val x2 = kv(2,2)
    val x3 = kv(3,3)
    
    def insert() = {
      implicit val t = new Tx
      
      val orig = l.root
      
      orig.content.isEmpty should be (true)
      
      orig.update(List(x2, x1), Nil, noSplit) flatMap { result =>
        t.commit()
      
        result map { r => r._1 }
      }
    }
    
    insert() flatMap { updatedNode => 
      implicit val t = new Tx
     
      updatedNode.content should be (SortedMap(x1, x2))
      
      updatedNode.update(List(x3), List(x1._1), noSplit) flatMap { result =>
        t.commit()
      
        result map { r => 
            r._1.content should be (SortedMap(x2, x3))
            r._2 should be (None)
        }
      }
    }
  }
  
  
  test("Overwrite") {
    val l = new TList(35)
    
    val x1 = kv(1,1)
    val x2 = kv(2,2)
    val x3 = kv(3,3)
    val x4 = kv(4,4)
    val x5 = kv(5,5)
    
    def insert() = {
      implicit val t = new Tx
      
      val orig = l.root
      
      orig.content.isEmpty should be (true)
      
      orig.update(List(x2, x1, x3), Nil, noSplit) flatMap { result =>
        t.commit()
      
        result map { r => r._1 }
      }
    }
    
    insert() flatMap { updatedNode => 
      implicit val t = new Tx
     
      updatedNode.content should be (SortedMap(x1, x2, x3))
      
      updatedNode.update(List(x4,x5), List(x1._1, x2._1, x3._1), noSplit) flatMap { result =>
        t.commit()
      
        result map { r => 
          val expected = SortedMap(x4, x5)
          r._1.content should be (expected)
          r._2 should be (None)
          
          t.overwriteOp should not be (null)
          toKVset(t.overwriteOp.content) should be (toKVset(expected))
          t.overwriteOp.rightPointer should be (None)
        }
      }
    }
  }
  
  test("Split") {
    implicit val t = new Tx
    val l = new TList(250)
    val orig = l.root
    
    orig.content.isEmpty should be (true)
    
    val ins = (1 to 25).map(i => kv(i,i)).toList
    
    val (left, right) = ins.splitAt(25/2)
    
    var splitCalled = false
    
    def onSplit(t:Transaction, ec:ExecutionContext, orig:KVListNode, updated:KVListNode, newNode:KVListNode): Unit = {
      splitCalled = true  
    }
    
    orig.update(ins, Nil, onSplit) flatMap { result =>
      t.commit()
    
      result map { r => 
        splitCalled should be (true)
        val leftContent = left.foldLeft(SortedMap[Array[Byte],Array[Byte]]())((m, t) => m + t)
        val rightContent = right.foldLeft(SortedMap[Array[Byte],Array[Byte]]())((m, t) => m + t)
        val rPtr = KVListNodePointer(mkptr(1), right.head._1)
        
        r._1.content should be (leftContent)
        r._1.rightNode.isEmpty should be (false)
        r._1.rightNode.get should be (rPtr)
        r._2 should not be (None)
        r._2.get.content should be (rightContent)
        
        var (rdecode, rptr) = KVListCodec.decodeNodeContent(l.compareKeys, l.m(mkptr(1).uuid).data)
        
        toKVset(t.overwriteOp.content) should be (toKVset(leftContent))
        
        t.overwriteOp.rightPointer.get.objectPointer should be (rPtr.objectPointer)
        t.overwriteOp.rightPointer.get.minimum should be (rPtr.minimum)
        
        toKVset(rdecode) should be (toKVset(rightContent))
        
        rptr should be (None)
      }
    }
  }
}