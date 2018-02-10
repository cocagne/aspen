package com.ibm.aspen.base.keyvalue

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



class KeyValueListSuite extends TestSystemSuite {
  import Bootstrap._
  
  def alloc(min: Option[Key], max: Option[Key], right: Option[KeyValueObjectPointer]): Future[KeyValueObjectPointer] = {
    implicit val tx = sys.newTransaction()
    
    var ops = List[KeyValueOperation]()
    
    min.foreach { m => ops = SetMin(m) :: ops }
    max.foreach { m => ops = SetMax(m) :: ops }
    right.foreach { m => ops = SetRight(m.toArray) :: ops }
    
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
  
  test("Test single-node scan") {
  
    val target = Key(Array[Byte](2))
    
    for {
      l0 <- alloc(None, None, None)
      
      lptr = KeyValueListPointer(KeyValueListPointer.AbsoluteMinimum, l0)
      
      kvos <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, target)
     
    } yield {
      kvos.pointer should be (l0) 
    }
  }
  
  test("Test two-node scan") {
  
    val max0 = Key(Array[Byte](5))
    val target = Key(Array[Byte](9))
    
    for {
      l1 <- alloc(Some(max0), None, None)
      l0 <- alloc(None, Some(max0), Some(l1))
      
      lptr = KeyValueListPointer(KeyValueListPointer.AbsoluteMinimum, l0)
      
      kvos <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, target)
     
    } yield {
      kvos.pointer should be (l1) 
    }
  }
  
  test("Test three-node scan, find middle") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](9))
    
    for {
      l2 <- alloc(Some(max1), None, None)
      l1 <- alloc(Some(max0), Some(max1), Some(l2))
      l0 <- alloc(None, Some(max0), Some(l1))
      
      lptr = KeyValueListPointer(KeyValueListPointer.AbsoluteMinimum, l0)
      
      kvos <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, target)
     
    } yield {
      kvos.pointer should be (l1) 
    }
  }
  
  test("Test three-node scan, find middle with target key equal to node minimum") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](5))
    
    for {
      l2 <- alloc(Some(max1), None, None)
      l1 <- alloc(Some(max0), Some(max1), Some(l2))
      l0 <- alloc(None, Some(max0), Some(l1))
      
      lptr = KeyValueListPointer(KeyValueListPointer.AbsoluteMinimum, l0)
      
      kvos <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, target)
     
    } yield {
      kvos.pointer should be (l1) 
    }
  }
}