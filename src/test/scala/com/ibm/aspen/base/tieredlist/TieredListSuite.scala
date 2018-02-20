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
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.base.ObjectReader
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.StorePointer

object TieredListSuite {
  val kvalue = Array[Byte](1,2)
  
  class TKVL(val sys: AspenSystem, depth: Int, root: KeyValueObjectPointer) extends TieredKeyValueList {
    val keyOrdering: KeyOrdering = ByteArrayKeyOrdering
    
    override protected def rootPointer()(implicit ec: ExecutionContext): Future[TieredKeyValueList.Root] = Future.successful(TieredKeyValueList.Root(depth, root))
  
    override protected def getObjectReaderForTier(tier: Int): ObjectReader = sys
    
    def find(key: Key, targetTier: Int=0)(implicit ec: ExecutionContext): Future[KeyValueObjectState]  = fetchContainingNode(key, targetTier)
  }
}

class TieredListSuite extends TestSystemSuite {
  import Bootstrap._
  import TieredListSuite._
  
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
  
  test("Test single-node tiered list") {
  
    val target = Key(Array[Byte](2))
    
    for {
      l0 <- alloc(None, None, None)
      
      tl = new TKVL(sys, 0, l0)
      
      kvos <- tl.find(target)
    
    } yield {
      kvos.pointer should be (l0) 
    }
  }
  
  test("Test two-node single tiered list") {
  
    val max0 = Key(Array[Byte](5))
    val target = Key(Array[Byte](9))
    
    for {
      l1 <- alloc(Some(max0), None, None)
      l0 <- alloc(None, Some(max0), Some(l1))
      
      tl = new TKVL(sys, 0, l0)
      
      kvos <- tl.find(target)
    } yield {
      kvos.pointer should be (l1) 
    }
  }
  
  test("Test three-node tiered list, 2 layers proper tree structure") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](9))
    
    for {
      l1 <- alloc(Some(max0), None, None)
      l0 <- alloc(None, Some(max0), Some(l1))
      a0 <- alloc(None, None, None, List((Key.AbsoluteMinimum,l0.toArray), (max0,l1.toArray)))
      
      tl = new TKVL(sys, 1, a0)
      
      kvos <- tl.find(target)
     
    } yield {
      kvos.pointer should be (l1) 
    }
  }
  
  test("Test three-node tiered list, 2 layers missing direct pointer") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](9))
    
    for {
      l1 <- alloc(Some(max0), None, None)
      l0 <- alloc(None, Some(max0), Some(l1))
      a0 <- alloc(None, None, None, List((Key.AbsoluteMinimum,l0.toArray)))
      
      tl = new TKVL(sys, 1, a0)
      
      kvos <- tl.find(target)
     
    } yield {
      kvos.pointer should be (l1) 
    }
  }
  
  test("Test 3 layers proper tree structure") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](9))
    
    for {
      l1 <- alloc(Some(max0), None, None)
      l0 <- alloc(None, Some(max0), Some(l1))
      a1 <- alloc(Some(max0), None, None, List((max0,l1.toArray)))
      a0 <- alloc(None, Some(max0), Some(a1), List((Key.AbsoluteMinimum,l0.toArray)))
      b0 <- alloc(None, None, None, List((Key.AbsoluteMinimum,a0.toArray), (max0,a1.toArray)))
      
      tl = new TKVL(sys, 2, b0)
      
      kvos <- tl.find(target)
     
    } yield {
      kvos.pointer should be (l1) 
    }
  }
  
  test("Test 3 layers proper tree structure, find tier1") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](9))
    
    for {
      l1 <- alloc(Some(max0), None, None)
      l0 <- alloc(None, Some(max0), Some(l1))
      a1 <- alloc(Some(max0), None, None, List((max0,l1.toArray)))
      a0 <- alloc(None, Some(max0), Some(a1), List((Key.AbsoluteMinimum,l0.toArray)))
      b0 <- alloc(None, None, None, List((Key.AbsoluteMinimum,a0.toArray), (max0,a1.toArray)))
      
      tl = new TKVL(sys, 2, b0)
      
      kvos <- tl.find(target, 1)
     
    } yield {
      kvos.pointer should be (a1) 
    }
  }
  
  test("Test 3 layers missing secondary pointers") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](9))
    
    for {
      l1 <- alloc(Some(max0), None, None)
      l0 <- alloc(None, Some(max0), Some(l1))
      a1 <- alloc(Some(max0), None, None, List((max0,l1.toArray)))
      a0 <- alloc(None, Some(max0), Some(a1), List((Key.AbsoluteMinimum,l0.toArray)))
      b0 <- alloc(None, None, None, List((Key.AbsoluteMinimum,a0.toArray)))
      
      tl = new TKVL(sys, 2, b0)
      
      kvos <- tl.find(target)
     
    } yield {
      kvos.pointer should be (l1) 
    }
  }
  
  test("Test 3 layers broken pointers, get(key)") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](9))
    
    val sp0 = StorePointer(0, List[Byte](0).toArray)
    val sp1 = StorePointer(1, List[Byte](1).toArray)
    val sp2 = StorePointer(2, List[Byte](2).toArray)
  
    val badPointer =  KeyValueObjectPointer(new UUID(0,99), new UUID(0,0), None, new Replication(3,2), (sp0 :: sp1 :: sp2 :: Nil).toArray)
    
    for {
      l1 <- alloc(Some(max0), None, None, List((target,kvalue)))
      l0 <- alloc(None, Some(max0), Some(l1))
      a1 <- alloc(Some(max0), None, None, List((max0,badPointer.toArray)))
      a0 <- alloc(None, Some(max0), Some(a1), List((Key.AbsoluteMinimum,l0.toArray)))
      b0 <- alloc(None, None, None, List((Key.AbsoluteMinimum,a0.toArray)))
      
      tl = new TKVL(sys, 2, b0)
      
      ov <- tl.get(target)
     
    } yield {
      ov match {
        case None => fail("failed to find target")
        case Some(v) => v.value should be (kvalue)
      }
    }
  }
}