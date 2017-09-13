package com.ibm.aspen.base.kvtree

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
import com.ibm.aspen.base.ObjectStateAndData
import com.ibm.aspen.base.kvlist.KVList
import com.ibm.aspen.base.kvlist.KVListNodeAllocater
import com.ibm.aspen.base.SimpleTestSystem
import com.ibm.aspen.base.kvlist.KVListNodePointer
import com.ibm.aspen.core.network.{Codec => NetworkCodec}
import com.ibm.aspen.base.kvlist.KVListCodec
import com.ibm.aspen.base.kvlist.KVListNode

object KVTreeSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
  val poolUUID = new UUID(0,0)
  val treePolicyUUID = new UUID(1,2)
  
  def mkptr(objectNum:Int) = ObjectPointer(new UUID(0,objectNum), poolUUID, None, Replication(3,2), new Array[StorePointer](0)) 
  
  def np(minimum: Array[Byte], ptr: ObjectPointer): KVListNodePointer = KVListNodePointer(ptr, minimum)
  
  import scala.language.implicitConversions
  
  implicit def iarr(i: Int): Array[Byte] = {
    val bb = ByteBuffer.allocate(4)
    bb.putInt(i)
    bb.position(0)
    bb.array
  }
  def a2i(a: Array[Byte]): Int = ByteBuffer.wrap(a).getInt
  
  def kv(key:Int, value:Int): (Array[Byte], Array[Byte]) = (iarr(key), iarr(value))
  
  def compareKeysFn(a: Array[Byte], b: Array[Byte]): Int = {
    val ia = ByteBuffer.wrap(a).getInt()
    val ib = ByteBuffer.wrap(b).getInt()
    ia - ib
  }
  
  def toKVset(a:SortedMap[Array[Byte], Array[Byte]]): Set[(Int,Int)] = {
    def dc(b: Array[Byte]): Int = ByteBuffer.wrap(b).getInt
    a.iterator.map(t => (dc(t._1), dc(t._2))).toSet
  }
  
  implicit val keyOrdering = new KVList.KeyOrdering(compareKeysFn)
  
  class TestSetup {
    val system = new SimpleTestSystem
    
    def mknode(content: List[KVListNodePointer], rptr: Option[KVListNodePointer]): Future[ObjectPointer] = {
      implicit val tx = new system.Tx
      val f = system.allocateObject(mkptr(0), ObjectRevision(0,0), new UUID(0,0), 0, KVTreeCodec.encode(content, rptr))
      tx.commit()
      f
    }
    
    def mkleaf(content: List[(Array[Byte],Array[Byte])], rptr: Option[KVListNodePointer]): Future[ObjectPointer] = {
      implicit val tx = new system.Tx
      val data = KVListCodec.testEncodeContent(content, rptr)
      val f = system.allocateObject(mkptr(0), ObjectRevision(0,0), new UUID(0,0), 0, data)
      tx.commit()
      f
    }
    
    def mktree(tiers: List[ObjectPointer]): Future[ObjectPointer] = {
      implicit val tx = new system.Tx
      val data = ByteBuffer.wrap(KVTreeCodec.encodeTreeDescription(treePolicyUUID, tiers))
      val f = system.allocateObject(mkptr(0), ObjectRevision(0,0), new UUID(0,0), 0, data)
      tx.commit()
      f
    }
    
    class ListAlloc(val nodeSizeLimit: Int) extends KVListNodeAllocater {
       
      val allocationPolicyUUID: UUID = new UUID(100,100)
      
      def allocate(
          targetObject:ObjectPointer, targetRevision: ObjectRevision, 
          initialContent: ByteBuffer)(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer] = {
        system.allocateObject(targetObject, ObjectRevision(0,0), new UUID(0,0), 0, initialContent)
      }
    }
    
    class TreeAlloc(val nodeSizeLimit: Int=10000) extends KVTreeNodeAllocater {
  
      val allocationPolicyUUID: UUID = new UUID(100,100)
      
      def allocateRootTierNode(
          targetObject: ObjectPointer, targetRevision: ObjectRevision, 
          newTier: Int, initialContent: List[KVListNodePointer])(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer] = {
        system.allocateObject(targetObject, ObjectRevision(0,0), new UUID(0,0), 0, KVTreeCodec.encode(initialContent, None))
      }
      
      def allocateRootLeafNode(
          targetObject: ObjectPointer, targetRevision: ObjectRevision)(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer] = {
        system.allocateObject(targetObject, ObjectRevision(0,0), new UUID(0,0), 0, ByteBuffer.allocate(0))
      }
      
      def getListNodeAllocaterForTier(tier: Int): KVListNodeAllocater = new ListAlloc(nodeSizeLimit)
    }
    
    class TestList(val rootObjectPointer: ObjectPointer, val nodeSizeLimit: Int) extends KVList {
   
      def fetchNodeObject(objectPointer: ObjectPointer): Future[ObjectStateAndData] = system.readObject(objectPointer, None)
      
      def compareKeys(a: Array[Byte], b: Array[Byte]): Int = compareKeysFn(a, b)
      
      val objectAllocater = new ListAlloc(nodeSizeLimit)
    }
  }
  
}

class KVTreeSuite extends AsyncFunSuite with Matchers {
  import KVTreeSuite._
  
  test("Test TestSetup") {
    val ts = new TestSetup()
    
    for {
      l3 <- ts.mkleaf( kv(10,10)::Nil, None)
      l2 <- ts.mkleaf( kv(5,5)::Nil, Some(KVListNodePointer(l3,10)))
      l1 <- ts.mkleaf( Nil, Some(KVListNodePointer(l2,3)))
      lst = new ts.TestList(l1, 100)
      root <- lst.fetchRootNode()
      cont <- root.fetchContainingNode(10)
    } yield {
      cont.content.contains(10) should be (true)
    }
  }
  
  test("Test broken hierarchy") {
    val ts = new TestSetup()
    var lastResortCalled = false
    
    for {
      l3 <- ts.mkleaf( kv(10,10)::Nil, None)
      l2 <- ts.mkleaf( kv(5,5)::Nil, Some(KVListNodePointer(l3,10)))
      l1 <- ts.mkleaf( Nil, Some(KVListNodePointer(l2,3)))
      n1 <- ts.mknode(Nil, None)
      td <- ts.mktree(l1::n1::Nil)
      os <- ts.system.readObject(td)
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(9999), new KVTreeNodeCache {}, compareKeysFn, l1::n1::Nil, ts.system) {
        override def navigationFallbackOfLastResort(targetTier: Int, key: Array[Byte])(implicit ec: ExecutionContext): Future[KVListNode] = {
         lastResortCalled = true
         super.navigationFallbackOfLastResort(targetTier, key)
        }
      }  
      (tier, cont) <- tr.fetchContainingNode(10)
    } yield {
      lastResortCalled should be (true)
      cont.content.contains(10) should be (true)
    }
  }
  
  test("Test simple hierarchy navigation") {
    val ts = new TestSetup()
    
    for {
      l3 <- ts.mkleaf( kv(10,10)::Nil, None)
      l2 <- ts.mkleaf( kv(5,5)::Nil, Some(KVListNodePointer(l3,10)))
      l1 <- ts.mkleaf( Nil, Some(KVListNodePointer(l2,3)))
      n1 <- ts.mknode(np(0,l1)::np(3,l2)::np(7,l3)::Nil, None)
      td <- ts.mktree(l1::n1::Nil)
      os <- ts.system.readObject(td)
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(9999), new KVTreeNodeCache {}, compareKeysFn, l1::n1::Nil, ts.system)      
      (tier, cont) <- tr.fetchContainingNode(10)
    } yield {
      cont.content.contains(10) should be (true)
    }
  }
  
  test("Test multi-level hierarchy navigation") {
    val ts = new TestSetup()
    
    for {
      l3 <- ts.mkleaf( kv(10,10)::Nil, None)
      l2 <- ts.mkleaf( kv(5,5)::Nil, Some(KVListNodePointer(l3,10)))
      l1 <- ts.mkleaf( Nil, Some(KVListNodePointer(l2,5)))
      
      n3 <- ts.mknode(np(10,l3)::Nil, None)
      n2 <- ts.mknode(np(5,l2)::Nil, Some(np(10,n3)))
      n1 <- ts.mknode(np(0,l1)::Nil, Some(np(5,n2)))
      
      m1 <- ts.mknode(np(0,n1)::np(5,n2)::np(10,n3)::Nil, None)
      td <- ts.mktree(l1::n1::m1::Nil)
      os <- ts.system.readObject(td)
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(9999), new KVTreeNodeCache {}, compareKeysFn, l1::n1::m1::Nil, ts.system) {
        override def navigationFallbackOfLastResort(targetTier: Int, key: Array[Byte])(implicit ec: ExecutionContext): Future[KVListNode] = Future.failed(new Exception("Should not be used!")) 
      }
      (tier, cont) <- tr.fetchContainingNode(5)
    } yield {
      println("All node contents:")
      cont.content.foreach(t => println(s"${a2i(t._1)}, ${a2i(t._2)}"))
      cont.content.contains(5) should be (true)
    }
  }
}