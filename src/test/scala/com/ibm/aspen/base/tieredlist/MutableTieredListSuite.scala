package com.ibm.aspen.base.tieredlist

import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest._
import com.ibm.aspen.base.TestSystem
import com.ibm.aspen.base.TestSystemSuite
import com.ibm.aspen.base.impl.Bootstrap
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.util.UUID
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.SetMin
import com.ibm.aspen.core.objects.keyvalue.SetMax
import com.ibm.aspen.core.objects.keyvalue.SetRight
import com.ibm.aspen.base.impl.SinglePoolObjectAllocater
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.KeyValueObjectState


class MutableTieredListSuite extends TestSystemSuite {
  import Bootstrap._
  
  val treeKey = Key(Array[Byte](0,0,0))
  
  def alloc(min: Option[Key], max: Option[Key], right: Option[KeyValueObjectPointer], contents: List[(Key,Array[Byte])] = Nil): Future[KeyValueObjectPointer] = {
    
    implicit val tx = sys.newTransaction()

    var ops = List[KeyValueOperation]()
    
    min.foreach { m => ops = SetMin(m) :: ops }
    max.foreach { m => ops = SetMax(m) :: ops }
    right.foreach { m => ops = SetRight(m.toArray) :: ops }
    
    contents.foreach { t => ops = Insert(t._1, t._2) :: ops }
    
    for {
      r <- sys.readObject(sys.radiclePointer)
      
      // give transaction something to do
      meh = tx.bumpVersion(sys.radiclePointer, r.revision)
      
      kvp <- sys.lowLevelAllocateKeyValueObject(
            sys.radiclePointer, 
            ObjectRevision(UUID.randomUUID()), 
            BootstrapStoragePoolUUID, 
            None,
            TestSystem.DefaultIDA, 
            ops, 
            None)
            
      done <- tx.commit()
    } yield kvp
  }
  
  def mk(rootNode: KeyValueObjectPointer, nodeSizeLimit:Int=300, kvPairLimit:Int=100, topTier:Int=0): Future[MutableTieredKeyValueList] = {
    val allocaterType = SimpleTieredKeyValueListNodeAllocater.typeUUID
    val allocaterConfig = SimpleTieredKeyValueListNodeAllocater.encode(Array(BootstrapStoragePoolUUID), Array(nodeSizeLimit), Array(kvPairLimit))
    
    val root = TieredKeyValueListRoot(topTier, ByteArrayKeyOrdering, rootNode, allocaterType, allocaterConfig)
    
    alloc(None, None, None, List((treeKey -> root.toArray))).map { rootContainer =>
      val rootMgr = new MutableKeyValueObjectRootManager(sys, rootContainer, treeKey, root)
      new MutableTieredKeyValueList(rootMgr)
    }
  }
  
  def reload(tkvl: MutableTieredKeyValueList): Future[MutableTieredKeyValueList] = {
    val m = tkvl.rootManager.asInstanceOf[MutableKeyValueObjectRootManager]
    sys.readObject(m.containingObject) map { kvos =>
      val newRootMgr = new MutableKeyValueObjectRootManager(sys, m.containingObject, treeKey, TieredKeyValueListRoot(kvos.contents(treeKey).value))
      new MutableTieredKeyValueList(newRootMgr)
    }
  }
  
  def readContainerObject(tkvl: MutableTieredKeyValueList): Future[KeyValueObjectState] = {
    val m = tkvl.rootManager.asInstanceOf[MutableKeyValueObjectRootManager]
    sys.readObject(m.containingObject)
  }
  
  test("Test single-node tree") {
  
    val target = Key(Array[Byte](2))
    val value = Array[Byte](2,3,4)
    
    for {
      l0 <- alloc(None, None, None, List((target -> value)))
      
      tkvl <- mk(l0)

      ovalue <- tkvl.get(target)

    } yield {
      ovalue match {
        case None => fail("failed to find target key")
        case Some(v) => v.value should be (value)
      }
    }
  }
  
  test("Test replace non-existent value") {
  
    val target = Key(Array[Byte](2))
    val value = Array[Byte](2,3,4)
    
    val invalid = Key("NONEXISTENT")
    
    implicit val tx = sys.newTransaction()
    
    for {
      l0 <- alloc(None, None, None, List((target -> value)))
      
      nodeSizeLimit = 300
      kvPairLimit = 100
      
      tkvl <- mk(l0)

      r <- tkvl.replace(invalid, target).failed

    } yield {
      r match {
        case err: KeyDoesNotExist => err.key should be (invalid)
        case _ => fail("failed to find target key")
      }
    }
  }
  
  test("Test replace value. Single node") {

    val target = Key(Array[Byte](2))
    val value = Array[Byte](2,3,4)
    
    val old = Key("old")
    
    def put(tkvl: MutableTieredKeyValueList): Future[Unit] = {
      implicit val tx = sys.newTransaction()
      tkvl.put(old, value).flatMap(_ => tx.commit().map(_=>()))
    }
    
    def replace(tkvl: MutableTieredKeyValueList): Future[Unit] = {
      implicit val tx = sys.newTransaction()
      tkvl.replace(old, target).flatMap(_ => tx.commit().map(_=>()))
    }
    
    for {
      l0 <- alloc(None, None, None, List((target -> value)))
      
      tkvl <- mk(l0)

      _ <- put(tkvl)
      
      _ <- replace(tkvl)
      
      v <- tkvl.get(target)

    } yield {
      java.util.Arrays.equals(v.get.value, value) should be (true)
    }
  }
  
  test("Test split single-node tree") {
  
    val target = Key(Array[Byte](10))
    val value = Array[Byte](2,3,4)
    val key0 = Key(Array[Byte](0))
    val key1 = Key(Array[Byte](1))
    val key2 = Key(Array[Byte](2))
    val key3 = Key(Array[Byte](3))
    val key4 = Key(Array[Byte](4))
    val bulk = new Array[Byte](50)
    
    implicit val tx = sys.newTransaction()
    
    for {
      l0 <- alloc(None, None, None, List((key0 -> bulk), (key1 -> bulk), (key2 -> bulk), (key3 -> bulk)))
      
      tkvl <- mk(l0, kvPairLimit=4)
      
      node0 <- tkvl.fetchMutableNode(target)
      
      inserts = List((key4,bulk), (target,value))
      deletes = Nil
      requirements = Nil
      
      txPrepped <- node0.prepreUpdateTransaction(inserts, deletes, requirements)
      txDone <- tx.commit()
      
      finalizersDone <- waitForTransactionsComplete()

      ovalue0 <- tkvl.get(target)
      
      tkvl2 <- reload(tkvl)
      
      ovalue1 <- tkvl2.get(target)
      
      newRootKvos <- sys.readObject(tkvl2.rootManager.root.rootNode)

    } yield {
      tkvl.rootManager.root.topTier should be (0)
      tkvl2.rootManager.root.topTier should be (1)
      tkvl.rootManager.root.rootNode should be (l0)
      tkvl2.rootManager.root.rootNode should not be (l0)
      
      newRootKvos.contents.size should be (2)
      
      ovalue0 match {
        case None => fail("failed to find target key from original tree")
        case Some(v) => v.value should be (value)
      }
      ovalue1 match {
        case None => fail("failed to find target key from newly created tree")
        case Some(v) => v.value should be (value)
      }
      
    }
  }
  
  test("Test split two-tier tree") {

    val target = Key(Array[Byte](10))
    val value = Array[Byte](2,3,4)
    val key0 = Key(Array[Byte](0))
    val key1 = Key(Array[Byte](1))
    val key2 = Key(Array[Byte](2))
    val key3 = Key(Array[Byte](3))
    val key4 = Key(Array[Byte](4))
    val bulk = new Array[Byte](50)
    
    implicit val tx = sys.newTransaction()
    
    for {
      l0 <- alloc(None, None, None, List((key0 -> bulk), (key1 -> bulk), (key2 -> bulk), (key3 -> bulk)))
      
      rootPtr <- alloc(None, None, None, List((Key.AbsoluteMinimum -> l0.toArray)))
      
      tkvl <- mk(rootPtr, kvPairLimit=4, topTier=1)
       
      node0 <- tkvl.fetchMutableNode(target)
      
      inserts = List((key4,bulk), (target,value))
      deletes = Nil
      requirements = Nil
      
      txPrepped <- node0.prepreUpdateTransaction(inserts, deletes, requirements)
      txDone <- tx.commit()
      
      finalizersDone <- waitForTransactionsComplete()
      
      ovalue0 <- tkvl.get(target)
      
      tkvl2 <- reload(tkvl)
      
      ovalue1 <- tkvl2.get(target)
      
      newRootKvos <- sys.readObject(tkvl2.rootManager.root.rootNode)

    } yield {
      tkvl.rootManager.root.topTier should be (1)
      tkvl2.rootManager.root.topTier should be (1)
      tkvl.rootManager.root.rootNode should be (rootPtr)
      tkvl2.rootManager.root.rootNode should be (rootPtr)
      
      newRootKvos.contents.size should be (2)
      
      ovalue0 match {
        case None => fail("failed to find target key from original tree")
        case Some(v) => v.value should be (value)
      }
      ovalue1 match {
        case None => fail("failed to find target key from newly created tree")
        case Some(v) => v.value should be (value)
      }
      
    }
  }
  
  test("Test join two-tier tree") {
  
    val target = Key(Array[Byte](10))
    val value = Array[Byte](2,3,4)
    val key0 = Key(Array[Byte](0))
    
    implicit val tx = sys.newTransaction()
    
    for {
      l1 <- alloc(Some(target), None, None, List((target -> value)))
      l0 <- alloc(None, Some(target), Some(l1), List((key0 -> value)))
      
      rootPtr <- alloc(None, None, None, List((Key.AbsoluteMinimum -> l0.toArray), (target, l1.toArray)))
       
      tkvl <- mk(rootPtr, nodeSizeLimit=250, topTier=1)
      
      node0 <- tkvl.fetchMutableNode(key0)
      
      inserts = Nil
      deletes = List(key0)
      requirements = Nil
      
      txPrepped <- node0.prepreUpdateTransaction(inserts, deletes, requirements)
      txDone <- tx.commit()
      
      finalizersDone <- waitForTransactionsComplete()
      
      ovalue0 <- tkvl.get(key0)
      
      tkvl2 <- reload(tkvl)
      
      ovalue1 <- tkvl2.get(key0)
      
      newRootKvos <- sys.readObject(tkvl2.rootManager.root.rootNode)
      l0kvos <- sys.readObject(l0)

    } yield {
      tkvl.rootManager.root.topTier should be (1)
      tkvl2.rootManager.root.topTier should be (1)
      tkvl.rootManager.root.rootNode should be (rootPtr)
      tkvl2.rootManager.root.rootNode should be (rootPtr)
      
      newRootKvos.contents.size should be (1)
      
      l0kvos.maximum.isDefined should be (false)
      l0kvos.right.isDefined should be (false)
      
      ovalue0.isDefined should be (false)
      ovalue1.isDefined should be (false)
      
    }
  }
  
  test("Test destroy two-tier tree") {
  
    val target = Key(Array[Byte](10))
    val value = Array[Byte](2,3,4)
    val key0 = Key(Array[Byte](0))
    
    implicit val tx = sys.newTransaction()
    
    @volatile var keys = List[Key]()
    
    def prepDestroy(contents: Map[Key, Value]): Future[Unit] = {
      keys = keys ++ contents.keys
      Future.unit
    }
    
    for {
      l1 <- alloc(Some(target), None, None, List((target -> value)))
      l0 <- alloc(None, Some(target), Some(l1), List((key0 -> value)))
      
      rootPtr <- alloc(None, None, None, List((Key.AbsoluteMinimum -> l0.toArray), (target, l1.toArray)))
       
      tkvl <- mk(rootPtr, nodeSizeLimit=250, topTier=1)
      
      node0 <- tkvl.fetchMutableNode(key0)
      
      inserts = Nil
      deletes = List(key0)
      requirements = Nil
      
      txPrepped <- node0.prepreUpdateTransaction(inserts, deletes, requirements)
      txDone <- tx.commit()
      
      finalizersDone <- waitForTransactionsComplete()
      
      ovalue0 <- tkvl.get(key0)
      
      tkvl2 <- reload(tkvl)
      
      ovalue1 <- tkvl2.get(key0)
      
      newRootKvos <- sys.readObject(tkvl2.rootManager.root.rootNode)
      l0kvos <- sys.readObject(l0)
      
      toast <- tkvl2.destroy(prepDestroy)
      
      _ <- sys.readObject(l0).failed
      _ <- sys.readObject(l1).failed
      _ <- sys.readObject(rootPtr).failed
      
      containerKvos <- readContainerObject(tkvl)

    } yield {
      keys should be (List(target))
      containerKvos.contents.contains(treeKey) should be (false)
    }
  }
  
  test("Test destroy partially destroyed two-tier tree") {
  
    val treeId = Key(Array[Byte](0,0,0))
    val target = Key(Array[Byte](10))
    val value = Array[Byte](2,3,4)
    val key0 = Key(Array[Byte](0))
    
    implicit val tx = sys.newTransaction()
    
    @volatile var keys = List[Key]()
    
    def prepDestroy(contents: Map[Key, Value]): Future[Unit] = {
      keys = keys ++ contents.keys
      Future.unit
    }
    
    def listDestroy(kvos: KeyValueObjectState): Future[Unit] = Future.unit
    
    for {
      l1 <- alloc(Some(target), None, None, List((target -> value)))
      l0 <- alloc(None, Some(target), Some(l1), List((key0 -> value)))
      
      rootPtr <- alloc(None, None, None, List((Key.AbsoluteMinimum -> l0.toArray), (target, l1.toArray)))
      
      tkvl <- mk(rootPtr, nodeSizeLimit=250, topTier=1)

      node0 <- tkvl.fetchMutableNode(key0)
      
      inserts = Nil
      deletes = List(key0)
      requirements = Nil
      
      txPrepped <- node0.prepreUpdateTransaction(inserts, deletes, requirements)
      txDone <- tx.commit()
      
      finalizersDone <- waitForTransactionsComplete()
      
      ovalue0 <- tkvl.get(key0)
      
      tkvl2 <- reload(tkvl)
      
      ovalue1 <- tkvl2.get(key0)

      l0kvos <- sys.readObject(l0)
      
      tier0toast <- KeyValueList.destroy(sys, KeyValueListPointer(Key.AbsoluteMinimum, l0), listDestroy)
      
      _ <- sys.readObject(l0).failed
      
      toast <- tkvl2.destroy(prepDestroy)
      
      _ <- sys.readObject(l0).failed
      _ <- sys.readObject(l1).failed
      _ <- sys.readObject(rootPtr).failed

      containerKvos <- readContainerObject(tkvl)
    } yield {
      containerKvos.contents.contains(treeKey) should be (false)
    }
  }
  
  test("Test destroy single-node tree") {
  
    val treeId = Key(Array[Byte](0,0,0))
    val target = Key(Array[Byte](2))
    val value = Array[Byte](2,3,4)
    
    @volatile var keys = List[Key]()
    
    def prepDestroy(contents: Map[Key, Value]): Future[Unit] = {
      keys = keys ++ contents.keys
      Future.unit
    }
    
    for {
      l0 <- alloc(None, None, None, List((target -> value)))
      
      tkvl <- mk(l0)
      
      ovalue <- tkvl.get(target)
      
      toast <- tkvl.destroy(prepDestroy)
      
      _ <- sys.readObject(l0).failed

      containerKvos <- readContainerObject(tkvl)
    } yield {
      keys should be (List(target))
      containerKvos.contents.contains(treeKey) should be (false)
      ovalue match {
        case None => fail("failed to find target key")
        case Some(v) => v.value should be (value)
      }
    }
  }

}