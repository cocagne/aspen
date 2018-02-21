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

class SimpleMutableTieredListSuite extends TestSystemSuite {
  import Bootstrap._
  
  def alloc(min: Option[Key], max: Option[Key], right: Option[KeyValueObjectPointer], contents: List[(Key,Array[Byte])] = Nil): Future[KeyValueObjectPointer] = {
    
    implicit val tx = sys.newTransaction()

    var ops = List[KeyValueOperation]()
    
    min.foreach { m => ops = SetMin(m) :: ops }
    max.foreach { m => ops = SetMax(m) :: ops }
    right.foreach { m => ops = SetRight(m.toArray) :: ops }
    
    contents.foreach { t => ops = Insert(t._1, t._2, tx.timestamp()) :: ops }
    
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
  
  test("Test single-node tree") {
  
    val treeId = Key(Array[Byte](0,0,0))
    val target = Key(Array[Byte](2))
    val value = Array[Byte](2,3,4)
    
    for {
      l0 <- alloc(None, None, None, List((target -> value)))
      
      nodeSizeLimit = 300
      
      root = TieredKeyValueList.Root(0, Array[UUID](BootstrapStoragePoolUUID), Array[Int](nodeSizeLimit), l0)
      
      rootContainer <- alloc(None, None, None, List((treeId -> root.toArray)))
      
      smt = new SimpleMutableTieredKeyValueList(sys, Left(rootContainer), treeId, ByteArrayKeyOrdering)
      
      ovalue <- smt.get(target)

    } yield {
      ovalue match {
        case None => fail("failed to find target key")
        case Some(v) => v.value should be (value)
      }
    }
  }
  
  test("Test split single-node tree") {
  
    val treeId = Key(Array[Byte](0,0,0))
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
      
      nodeSizeLimit = 250
      
      root = TieredKeyValueList.Root(0, Array[UUID](BootstrapStoragePoolUUID), Array[Int](nodeSizeLimit), l0)
      
      rootContainer <- alloc(None, None, None, List((treeId -> root.toArray)))
      
      smt = new SimpleMutableTieredKeyValueList(sys, Left(rootContainer), treeId, ByteArrayKeyOrdering)
      
      node0 <- smt.fetchMutableNode(target)
      
      inserts = List((key4,bulk), (target,value))
      deletes = Nil
      requirements = Nil
      
      txPrepped <- node0.prepreUpdateTransaction(inserts, deletes, requirements)
      txDone <- tx.commit()
      
      finalizersDone <- waitForTransactionsComplete()
      
      (newKvos, newRoot) <- smt.refreshRoot()
      
      ovalue0 <- smt.get(target)
      
      smt2 = new SimpleMutableTieredKeyValueList(sys, Left(rootContainer), treeId, ByteArrayKeyOrdering)
      
      ovalue1 <- smt2.get(target)
      
      newRootKvos <- sys.readObject(newRoot.rootNode)
      

    } yield {
      newRoot.topTier should be (1)
      newRoot.rootNode should not be (l0)
      
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
  
    val treeId = Key(Array[Byte](0,0,0))
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
      
      rootPtr <- alloc(None, None, None, List((KeyValueListPointer.AbsoluteMinimum -> l0.toArray)))
      
      tst <- sys.readObject(rootPtr)
       
      nodeSizeLimit = 250
      
      root = TieredKeyValueList.Root(1, Array[UUID](BootstrapStoragePoolUUID), Array[Int](nodeSizeLimit), rootPtr)
      
      rootContainer <- alloc(None, None, None, List((treeId -> root.toArray)))
      
      smt = new SimpleMutableTieredKeyValueList(sys, Left(rootContainer), treeId, ByteArrayKeyOrdering)
      
      node0 <- smt.fetchMutableNode(target)
      
      inserts = List((key4,bulk), (target,value))
      deletes = Nil
      requirements = Nil
      
      txPrepped <- node0.prepreUpdateTransaction(inserts, deletes, requirements)
      txDone <- tx.commit()
      
      finalizersDone <- waitForTransactionsComplete()
      
      (newKvos, newRoot) <- smt.refreshRoot()
      
      ovalue0 <- smt.get(target)
      
      smt2 = new SimpleMutableTieredKeyValueList(sys, Left(rootContainer), treeId, ByteArrayKeyOrdering)
      
      ovalue1 <- smt2.get(target)
      
      newRootKvos <- sys.readObject(newRoot.rootNode)

    } yield {
      newRoot.topTier should be (1)
      newRoot.rootNode should be (rootPtr)
      
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
  
  
}