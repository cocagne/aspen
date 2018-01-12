package com.ibm.aspen.base.kvtree

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
import com.ibm.aspen.base.kvlist.KVList
import com.ibm.aspen.base.kvlist.KVListNodeAllocater
import com.ibm.aspen.base.SimpleTestSystem
import com.ibm.aspen.base.kvlist.KVListNodePointer
import com.ibm.aspen.base.kvlist.KVListCodec
import com.ibm.aspen.base.kvlist.KVListNode
import com.ibm.aspen.base.RetryStrategy
import com.ibm.aspen.base.FinalizationAction
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp

object KVTreeSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
  val poolUUID = new UUID(0,0)
  val treePolicyUUID = new UUID(1,2)
  val ida = new Replication(3,2)
  
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
  
  def toKVset(a:SortedMap[Array[Byte], Array[Byte]]): Set[(Int,Int)] = {
    def dc(b: Array[Byte]): Int = ByteBuffer.wrap(b).getInt
    a.iterator.map(t => (dc(t._1), dc(t._2))).toSet
  }
  
  val compareKeysFn = KVTree.getKeyComparisonFunction(KVTree.KeyComparison.BigInt)
  
  implicit val keyOrdering = new KVList.KeyOrdering(compareKeysFn)
  
  class TestSetup {
    import ExecutionContext.Implicits.global
    
    val system = new SimpleTestSystem
    
    def mknode(content: List[KVListNodePointer], rptr: Option[KVListNodePointer]): Future[ObjectPointer] = {
      implicit val tx = new system.Tx
      val f = system.lowLevelAllocateObject(mkptr(0), ObjectRevision.Null, new UUID(0,0), None, ida, KVTreeCodec.encode(content, rptr))
      tx.commit()
      f
    }
    
    def mkleaf(content: List[(Array[Byte],Array[Byte])], rptr: Option[KVListNodePointer]): Future[ObjectPointer] = {
      implicit val tx = new system.Tx
      val data = KVListCodec.testEncodeContent(content, rptr)
      val f = system.lowLevelAllocateObject(mkptr(0), ObjectRevision.Null, new UUID(0,0), None, ida, data)
      tx.commit()
      f
    }
    
    def mktree(tiers: List[ObjectPointer]): Future[ObjectPointer] = {
      implicit val tx = new system.Tx
      val td = KVTreeDefinition(treePolicyUUID, KVTree.KeyComparison.BigInt, tiers)
      val data = KVTreeCodec.encodeTreeDefinition(td)
      val f = system.lowLevelAllocateObject(mkptr(0), ObjectRevision.Null, new UUID(0,0), None, ida, DataBuffer(data))
      tx.commit()
      f
    }
    
    class ListAlloc(val nodeSizeLimit: Int) extends KVListNodeAllocater {
       
      val allocationPolicyUUID: UUID = new UUID(100,100)
      
      def allocate(
          targetObject:ObjectPointer, targetRevision: ObjectRevision, 
          initialContent: DataBuffer,
          timestamp: HLCTimestamp)(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer] = {
        system.lowLevelAllocateObject(targetObject, ObjectRevision.Null, new UUID(0,0), None, ida, initialContent, Some(timestamp))(t, ec)
      }
    }
    
    class TreeAlloc(val nodeSizeLimit: Int=10000) extends KVTreeNodeAllocater {
  
      val allocationPolicyUUID: UUID = new UUID(100,100)
      
      def allocateRootTierNode(
          targetObject: ObjectPointer, targetRevision: ObjectRevision, 
          newTier: Int, initialContent: List[KVListNodePointer])(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer] = {
        system.lowLevelAllocateObject(targetObject, ObjectRevision.Null, new UUID(0,0), None, ida, KVTreeCodec.encode(initialContent, None))(t, ec)
      }
      
      def allocateRootLeafNode(
          targetObject: ObjectPointer, targetRevision: ObjectRevision)(implicit ec: ExecutionContext, t: Transaction): Future[ObjectPointer] = {
        system.lowLevelAllocateObject(targetObject, ObjectRevision.Null, new UUID(0,0), None, ida, DataBuffer(ByteBuffer.allocate(0)))(t, ec)
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
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(9999), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, l1::n1::Nil, ts.system) {
        override def navigationFallbackOfLastResort(targetTier: Int, key: Array[Byte])(implicit ec: ExecutionContext): Future[KVListNode] = {
         lastResortCalled = true
         super.navigationFallbackOfLastResort(targetTier, key)(ec)
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
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(9999), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, l1::n1::Nil, ts.system)      
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
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(9999), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, l1::n1::m1::Nil, ts.system) {
        override def navigationFallbackOfLastResort(targetTier: Int, key: Array[Byte])(implicit ec: ExecutionContext): Future[KVListNode] = Future.failed(new Exception("Should not be used!")) 
      }
      (tier, cont) <- tr.fetchContainingNode(5)
    } yield {
      //cont.content.foreach(t => println(s"${a2i(t._1)}, ${a2i(t._2)}"))
      cont.content.contains(5) should be (true)
    }
  }
  
  test("Test invalid pointer during navigation") {
    val ts = new TestSetup()
    
    val invalidPointer = np(5, mkptr(999))
    
    for {
      l3 <- ts.mkleaf( kv(10,10)::Nil, None)
      l2 <- ts.mkleaf( kv(5,5)::Nil, Some(KVListNodePointer(l3,10)))
      l1 <- ts.mkleaf( Nil, Some(KVListNodePointer(l2,5)))
      
      n3 <- ts.mknode(np(10,l3)::Nil, None)
      n2 <- ts.mknode(np(3,l2)::invalidPointer::Nil, Some(np(10,n3)))
      n1 <- ts.mknode(np(0,l1)::Nil, Some(np(3,n2)))
      
      m1 <- ts.mknode(np(0,n1)::np(5,n2)::np(10,n3)::Nil, None)
      td <- ts.mktree(l1::n1::m1::Nil)
      os <- ts.system.readObject(td)
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(9999), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, l1::n1::m1::Nil, ts.system) {
        override def navigationFallbackOfLastResort(targetTier: Int, key: Array[Byte])(implicit ec: ExecutionContext): Future[KVListNode] = Future.failed(new Exception("Should not be used!")) 
      }
      (tier, cont) <- tr.fetchContainingNode(5) 
    } yield {
      //println("All node contents:")
      //cont.content.foreach(t => println(s"${a2i(t._1)}, ${a2i(t._2)}"))
      cont.content.contains(5) should be (true)
    }
  }
  
  test("Test backup to parent node during navigation") {
    val ts = new TestSetup()
    
    val invalidPointer1 = np(3, mkptr(998))
    val invalidPointer2 = np(5, mkptr(999))
    
    for {
      l3 <- ts.mkleaf( kv(10,10)::Nil, None)
      l2 <- ts.mkleaf( kv(5,5)::Nil, Some(KVListNodePointer(l3,10)))
      l1 <- ts.mkleaf( Nil, Some(KVListNodePointer(l2,5)))
      
      n3 <- ts.mknode(np(10,l3)::Nil, None)
      n2 <- ts.mknode(invalidPointer1::invalidPointer2::Nil, Some(np(10,n3)))
      n1 <- ts.mknode(np(0,l1)::Nil, Some(np(3,n2)))
      
      m1 <- ts.mknode(np(0,n1)::np(3,n2)::np(10,n3)::Nil, None)
      td <- ts.mktree(l1::n1::m1::Nil)
      os <- ts.system.readObject(td)
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(9999), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, l1::n1::m1::Nil, ts.system) {
        override def navigationFallbackOfLastResort(targetTier: Int, key: Array[Byte])(implicit ec: ExecutionContext): Future[KVListNode] = Future.failed(new Exception("Should not be used!")) 
      }
      (tier, cont) <- tr.fetchContainingNode(5)
    } yield {
      //println("All node contents:")
      //cont.content.foreach(t => println(s"${a2i(t._1)}, ${a2i(t._2)}"))
      cont.content.contains(5) should be (true)
    }
  }
  
  test("Test first node creation") {
    val ts = new TestSetup()
    
    for {
      td <- ts.mktree(Nil)
      os <- ts.system.readObject(td)
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(9999), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, Nil, ts.system) {
        override def navigationFallbackOfLastResort(targetTier: Int, key: Array[Byte])(implicit ec: ExecutionContext): Future[KVListNode] = Future.failed(new Exception("Should not be used!"))
      }  
      (tier, cont) <- tr.fetchContainingNode(10)
      os2 <- ts.system.readObject(td)
    } yield {
      val td2 = KVTreeCodec.decodeTreeDefinition(os2.data)
      os2.revision shouldNot be (os.revision)
      td2.tiers.isEmpty should be (false)
      cont.content.isEmpty should be (true)
    }
  }
  
  test("Test put and get") {
    val ts = new TestSetup()
    val k = iarr(5)
    val v = iarr(6)
    implicit val tx = ts.system.newTransaction()
    
    for {
      td <- ts.mktree(Nil)
      os <- ts.system.readObject(td)
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(9999), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, Nil, ts.system) {
        override def navigationFallbackOfLastResort(targetTier: Int, key: Array[Byte])(implicit ec: ExecutionContext): Future[KVListNode] = Future.failed(new Exception("Should not be used!"))
      }
      
      tx2 = ts.system.newTransaction()
      putReady <- tr.put(k,v)(executionContext, tx2)
      fcommit <- tx2.commit()
      
      v2 <- tr.get(k)
      os2 <- ts.system.readObject(td)
    } yield {
      val td2 = KVTreeCodec.decodeTreeDefinition(os2.data)
      os2.revision should not be (os.revision)
      v2.isEmpty should be (false)
      a2i(v2.get) should be (6)
    }
  }
  
  test("Test tier-0 split adds finalization action to transaction") {
    val ts = new TestSetup()
    val k1 = iarr(1)
    val k2 = iarr(10)
    val v = new Array[Byte](500)
    val tx1 = ts.system.newTransaction()
    val tx2 = ts.system.newTransaction().asInstanceOf[ts.system.Tx]
    
    for {
      l <- ts.mkleaf( (k1,v)::Nil, None)
      td <- ts.mktree(l::Nil)
      os <- ts.system.readObject(td)
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(800), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, l::Nil, ts.system)
      
      put1 <- tr.put(k1,v)(executionContext, tx1)
      commit1 <- tx1.commit()
      
      put2 <- tr.put(k2,v)(executionContext, tx2)
      commit1 <- tx2.commit()
      
    } yield {
      tx2.fas.isEmpty should be (false)
      tx2.fas.contains(KVTreeFinalizationActionHandler.InsertIntoUpperTierUUID) should be (true)
      val fa = KVTreeCodec.decodeInsertIntoUpperTierFinalizationAction(tx2.fas(KVTreeFinalizationActionHandler.InsertIntoUpperTierUUID))
      fa.treeDefinitionPointer should be (td)
      fa.targetTier should be (1)
      fa.nodePointer.objectPointer should not be (l)
    }
  }
  
  test("Test Finalization Action tier1") {
    val ts = new TestSetup()
    val k  = iarr(1)
    val v = iarr(10)
    
    val noRetry = new RetryStrategy {
      def retryUntilSuccessful[T](attempt: => Future[T]): Future[T] = {
        val p = Promise[T]()
        attempt onComplete {
          case Success(r) => p.success(r)
          case Failure(cause) => p.failure(cause)
        }
        p.future
      }
    }
    
    val treeFactory = new KVTreeFactory {
      override def createTree(treeDefinitionObject: ObjectPointer)(implicit executionContext: ExecutionContext): Future[KVTree] = for {
        osd <- ts.system.readObject(treeDefinitionObject)
      } yield {
        val tdef = KVTreeCodec.decodeTreeDefinition(osd.data)
        val tl = tdef.tiers.map( o => o.uuid )
        //println(s"Tiers: $tl")
        new KVTree(treeDefinitionObject, osd.revision, new ts.TreeAlloc(800), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, tdef.tiers, ts.system)
      }
    }
    
    val fah = new KVTreeFinalizationActionHandler(treeFactory, noRetry, ts.system)
    
    for {
      l <- ts.mkleaf( (k,v)::Nil, None)
      td <- ts.mktree(l::Nil)
      os <- ts.system.readObject(td)
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(800), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, l::Nil, ts.system)
      
      l2 <- ts.mkleaf((iarr(10),iarr(11))::Nil, None)      
      enc = KVTreeCodec.encodeInsertIntoUpperTierFinalizationAction(tr, 1, np(10,l2))
      fa = fah.createAction(enc)
      
      result <- fa.execute()
      
      tr2 <- treeFactory.createTree(td)
      (rootTier, rootNode) <- tr2.fetchContainingNode(new Array[Byte](0), 1) 
      (leafTier, leaf) <- tr2.fetchContainingNode(iarr(11), 0)
    } yield {
      tr2.numTiers should be (2)
      rootTier.tier should be (1)
      rootNode.content.contains(new Array[Byte](0)) should be (true)
      rootNode.content.contains(iarr(10)) should be (true)
      leafTier.tier should be (0)
      leaf.content.contains(iarr(10)) should be (true)
      val v = leaf.content(iarr(10))
      a2i(v) should be (11)
      leaf.nodePointer.objectPointer should be (l2)
    }
  }
  
  
  test("Test Finalization Action tier1 subsequent split") {
    val ts = new TestSetup()
    val k  = iarr(1)
    val v = iarr(10)
    
    val noRetry = new RetryStrategy {
      def retryUntilSuccessful[T](attempt: => Future[T]): Future[T] = {
        val p = Promise[T]()
        attempt onComplete {
          case Success(r) => p.success(r)
          case Failure(cause) => p.failure(cause)
        }
        p.future
      }
    }
    
    val treeFactory = new KVTreeFactory {
      def createTree(treeDefinitionObject: ObjectPointer)(implicit executionContext: ExecutionContext): Future[KVTree] = for {
        osd <- ts.system.readObject(treeDefinitionObject)
      } yield {
        val tdef = KVTreeCodec.decodeTreeDefinition(osd.data)
        val tl = tdef.tiers.map( o => o.uuid )
        new KVTree(treeDefinitionObject, osd.revision, new ts.TreeAlloc(800), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, tdef.tiers, ts.system)
      }
    }
    
    val fah = new KVTreeFinalizationActionHandler(treeFactory, noRetry, ts.system)
    
    for {
      l <- ts.mkleaf( (k,v)::Nil, None)
      td <- ts.mktree(l::Nil)
      os <- ts.system.readObject(td)
      tr = new KVTree(td, os.revision, new ts.TreeAlloc(800), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, l::Nil, ts.system)
      
      l2 <- ts.mkleaf((iarr(10),iarr(11))::Nil, None)      
      enc = KVTreeCodec.encodeInsertIntoUpperTierFinalizationAction(tr, 1, np(10,l2))
      fa = fah.createAction(enc)
      
      result <- fa.execute()
      
      tr2 <- treeFactory.createTree(td)
      
      l3 <- ts.mkleaf((iarr(20),iarr(20))::Nil, None)      
      enc2 = KVTreeCodec.encodeInsertIntoUpperTierFinalizationAction(tr2, 1, np(20,l3))
      fa2 = fah.createAction(enc2)
      
      result <- fa2.execute()
      
      tr3 <- treeFactory.createTree(td)
      
      (rootTier, rootNode) <- tr3.fetchContainingNode(new Array[Byte](0), 1) 
      (leafTier, leaf) <- tr3.fetchContainingNode(iarr(20), 0)
    } yield {
      tr2.numTiers should be (2)
      rootTier.tier should be (1)
      rootNode.content.contains(new Array[Byte](0)) should be (true)
      rootNode.content.contains(iarr(10)) should be (true)
      rootNode.content.contains(iarr(20)) should be (true)
      leafTier.tier should be (0)
      leaf.content.contains(iarr(20)) should be (true)
      val v = leaf.content(iarr(20))
      a2i(v) should be (20)
      leaf.nodePointer.objectPointer should be (l3)
    }
  }
  
  test("Test Finalization Action upper tier split") {
    val ts = new TestSetup()
    val k  = iarr(1)
    val v = iarr(10)
    
    object split {
      private [this] var splits: List[(Int, KVListNodePointer)] = Nil
      def add(tier: Int, newNode:KVListNode): Unit = synchronized {
        splits = (tier, newNode.nodePointer) :: splits
      }
      def list = synchronized { splits }
    }
    
    val noRetry = new RetryStrategy {
      def retryUntilSuccessful[T](attempt: => Future[T]): Future[T] = {
        val p = Promise[T]()
        attempt onComplete {
          case Success(r) => p.success(r)
          case Failure(cause) => p.failure(cause)
        }
        p.future
      }
    }
    
    trait SplitCapture extends KVTree {
      override def onListNodeSplit(tier: Int)(transaction:Transaction, ec:ExecutionContext, originalNode:KVListNode, updatedNode:KVListNode, newNode:KVListNode): Unit = {
        split.add(tier, newNode)
        super.onListNodeSplit(tier)(transaction, ec, originalNode, updatedNode, newNode)
      }
    }
    
    val treeFactory = new KVTreeFactory {
      def createTree(treeDefinitionObject: ObjectPointer)(implicit executionContext: ExecutionContext): Future[KVTree] = for {
        osd <- ts.system.readObject(treeDefinitionObject)
      } yield {
        val tdef = KVTreeCodec.decodeTreeDefinition(osd.data)
        val tl = tdef.tiers.map( o => o.uuid )
        new KVTree(treeDefinitionObject, osd.revision, new ts.TreeAlloc(250), new KVTreeNodeCache {}, KVTree.KeyComparison.BigInt, tdef.tiers, ts.system) with SplitCapture
      }
    }
    
    val fah = new KVTreeFinalizationActionHandler(treeFactory, noRetry, ts.system)
    
    for {
      l3 <- ts.mkleaf( kv(10,10)::Nil, None)
      l2 <- ts.mkleaf( kv(5,5)::Nil, Some(KVListNodePointer(l3,10)))
      l1 <- ts.mkleaf( Nil, Some(KVListNodePointer(l2,5)))
      
      t1 <- ts.mknode(np(0,l1)::np(5,l2)::Nil, None)
      
      td <- ts.mktree(l1::t1::Nil)
      
      // At 250 bytes in size, the single tier1 node can hold 2 pointers but not three. Adding a pointer 
      // to l3 will cause t1 to split and create a finalization action to create tier2
      tr <- treeFactory.createTree(td)
      enc = KVTreeCodec.encodeInsertIntoUpperTierFinalizationAction(tr, 1, np(10,l3))
      fa = fah.createAction(enc)
      
      result <- fa.execute()
      
      if split.list.length == 1  // this will cause test to fail if length isn't 1
      
      (tier, nodePointer) = split.list.head
      
      tr <- treeFactory.createTree(td)
      enc = KVTreeCodec.encodeInsertIntoUpperTierFinalizationAction(tr, tier, nodePointer)
      fa = fah.createAction(enc)
      
      result <- fa.execute()

      tr4 <- treeFactory.createTree(td)
      
    } yield {
      tr4.numTiers should be (3)
    }
  }
  
}