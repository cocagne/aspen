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
import com.ibm.aspen.core.allocation.ObjectAllocationRevisionGuard
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.KeyValueObjectState



class KeyValueListSuite extends TestSystemSuite {
  import Bootstrap._
  
  def alloc(min: Option[Key], max: Option[Key], right: Option[KeyValueObjectPointer], contents: List[Insert] = Nil): Future[KeyValueObjectPointer] = {
    implicit val tx = sys.newTransaction()
    
    var ops = List[KeyValueOperation]()
    
    min.foreach { m => ops = SetMin(m) :: ops }
    max.foreach { m => ops = SetMax(m) :: ops }
    right.foreach { m => ops = SetRight(m.toArray) :: ops }
    
    contents.foreach { i => ops = i :: ops }
    
    for {
      r <- sys.readObject(sys.radiclePointer)
      
      // give transaction something to do
      meh = tx.bumpVersion(sys.radiclePointer, r.revision)
      
      kvp <- sys.lowLevelAllocateKeyValueObject(
        ObjectAllocationRevisionGuard(sys.radiclePointer,
            ObjectRevision(UUID.randomUUID())),
            BootstrapStoragePoolUUID, 
            None,
            TestSystem.DefaultIDA, 
            ops)
            
      done <- tx.commit()
    } yield kvp
  }
  
  test("Test single-node scan") {
  
    val target = Key(Array[Byte](2))
    
    for {
      l0 <- alloc(None, None, None)
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
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
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
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
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
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
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
      kvos <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, target)
     
    } yield {
      kvos.pointer should be (l1) 
    }
  }
  
  test("Insert KeyValue pair") {
  
    val key = Key(Array[Byte](2))
    val value = Array[Byte](4,5)
    
    var split: KeyValueObjectPointer = null
    var join: KeyValueObjectPointer = null
    
    implicit val tx = sys.newTransaction()
    
    for {
      l0 <- alloc(None, None, None)
      
      kvos0 <- sys.readObject(l0)
      
      nodeSizeLimit = 50 + l0.toArray.size
      kvPairLimit = 100
      
      inserts = List((key,value))
      deletes = Nil
      requirements = Nil
      comparison = ByteArrayKeyOrdering
      reader = sys
      allocater = new SinglePoolObjectAllocater(sys, Bootstrap.BootstrapObjectAllocaterUUID, BootstrapStoragePoolUUID, Some(nodeSizeLimit), new Replication(3,2))
      onSplit = (left: KeyValueListPointer, right: List[KeyValueListPointer]) => split = right.head.pointer
      onJoin = (left: KeyValueListPointer, removed: KeyValueListPointer) => join = removed.pointer
      
      kvosPrep <- KeyValueList.prepreUpdateTransaction(kvos0, nodeSizeLimit, kvPairLimit, inserts, deletes, requirements, comparison, reader, allocater, onSplit, onJoin)
      
      done <- tx.commit()
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
      kvos <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, key)
     
    } yield {
      kvos.pointer should be (l0)
      kvos.contents.get(key) match {
        case None => fail("missing key")
        case Some(v) => v.value should be (value)
      }
    }
  }
  
  test("Test join node") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](5))
    val key0 = Key(Array[Byte](1))
    val key1 = Key(Array[Byte](2))
    val key2 = Key(Array[Byte](7))
    val value = new Array[Byte](1)
    
    var split: KeyValueObjectPointer = null
    var join: KeyValueObjectPointer = null
    
    implicit val tx = sys.newTransaction()
    
    for {
      l2 <- alloc(Some(max1), None, None)
      l1 <- alloc(Some(max0), Some(max1), Some(l2), List(Insert(key2,value)))
      l0 <- alloc(None, Some(max0), Some(l1), List(Insert(key0,value), Insert(key1,value)))
      
      kvos0 <- sys.readObject(l0)

      nodeSizeLimit = 220
      kvPairLimit = 100
      
      inserts = Nil
      deletes = List(key0, key1)
      requirements = Nil
      comparison = ByteArrayKeyOrdering
      reader = sys
      allocater = new SinglePoolObjectAllocater(sys, Bootstrap.BootstrapObjectAllocaterUUID, BootstrapStoragePoolUUID, Some(nodeSizeLimit), new Replication(3,2))
      onSplit = (left: KeyValueListPointer, right: List[KeyValueListPointer]) => split = right.head.pointer
      onJoin = (left: KeyValueListPointer, removed: KeyValueListPointer) => join = removed.pointer
      
      kvosPrep <- KeyValueList.prepreUpdateTransaction(kvos0, nodeSizeLimit, kvPairLimit, inserts, deletes, requirements, comparison, reader, allocater, onSplit, onJoin)
      
      done <- tx.commit()
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
      kvos <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, target)
     
    } yield {
      kvos.pointer should be (l0)
      kvos.contents.size should be (1)
      kvos.minimum should be (None)
      kvos.maximum.isDefined should be (true)
      kvos.maximum.get.key should be (max1)
      kvos.right match {
        case None => fail("missing pointer")
        case Some(r) => r.content should be (l2.toArray)
      }
      kvos.contents.get(key2) match {
        case None => fail("missing key")
        case Some(v) => v.value should be (value)
      }
      split should be (null)
      join should be (l1)
    }
  }
  
  test("Split end node") {
  
    val key0 = Key(Array[Byte](0))
    val key1 = Key(Array[Byte](1))
    val key2 = Key(Array[Byte](2))
    val key3 = Key(Array[Byte](3))
    val key4 = Key(Array[Byte](4))
    //val key5 = Key(Array[Byte](5))
    val value = new Array[Byte](5)
    
    var split: KeyValueObjectPointer = null
    var join: KeyValueObjectPointer = null
    
    implicit val tx = sys.newTransaction()
    
    for {
      l0 <- alloc(None, None, None)
      
      kvos0 <- sys.readObject(l0)
      
      nodeSizeLimit = 8192
      kvPairLimit = 3
      
      inserts = List((key0,value),(key1,value),(key2,value),(key3,value),(key4,value))
      deletes = Nil
      requirements = Nil
      comparison = ByteArrayKeyOrdering
      reader = sys
      allocater = new SinglePoolObjectAllocater(sys, Bootstrap.BootstrapObjectAllocaterUUID, BootstrapStoragePoolUUID, Some(nodeSizeLimit), new Replication(3,2))
      onSplit = (left: KeyValueListPointer, right: List[KeyValueListPointer]) => split = right.head.pointer
      onJoin = (left: KeyValueListPointer, removed: KeyValueListPointer) => join = removed.pointer
      
      kvosPrep <- KeyValueList.prepreUpdateTransaction(kvos0, nodeSizeLimit, kvPairLimit, inserts, deletes, requirements, comparison, reader, allocater, onSplit, onJoin)
      
      done <- tx.commit()
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
      kvos1 <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, key0)
      kvos2 <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, key4)
     
    } yield {
      kvos1.pointer should be (l0)
      kvos1.contents.size should be (2)
      kvos1.contents.get(key0) match {
        case None => fail("missing key")
        case Some(v) => v.value should be (value)
      }
      split shouldNot be (null)
      kvos2.pointer shouldNot be (l0)
      kvos2.contents.size should be (3)
      kvos2.contents.get(key4) match {
        case None => fail("missing key")
        case Some(v) => v.value should be (value)
      }
    }
  }
 
  test("Split middle node") {
  
    val key0 = Key(Array[Byte](0))
    val key1 = Key(Array[Byte](1))
    val key2 = Key(Array[Byte](2))
    val key3 = Key(Array[Byte](3))
    val key4 = Key(Array[Byte](4))
    val max0 = Key(Array[Byte](5))
    val value = new Array[Byte](50)
    
    var split: KeyValueObjectPointer = null
    var join: KeyValueObjectPointer = null
    
    implicit val tx = sys.newTransaction()
    
    for {
      l1 <- alloc(Some(max0), None, None)
      l0 <- alloc(None, Some(max0), Some(l1))
      
      kvos0 <- sys.readObject(l0)
      
      nodeSizeLimit = 8192
      kvPairLimit = 3
      
      inserts = List((key0,value),(key1,value),(key2,value),(key3,value),(key4,value))
      deletes = Nil
      requirements = Nil
      comparison = ByteArrayKeyOrdering
      reader = sys
      allocater = new SinglePoolObjectAllocater(sys, Bootstrap.BootstrapObjectAllocaterUUID, BootstrapStoragePoolUUID, Some(nodeSizeLimit), new Replication(3,2))
      onSplit = (left: KeyValueListPointer, right: List[KeyValueListPointer]) => split = right.head.pointer
      onJoin = (left: KeyValueListPointer, removed: KeyValueListPointer) => join = removed.pointer
      
      kvosPrep <- KeyValueList.prepreUpdateTransaction(kvos0, nodeSizeLimit, kvPairLimit, inserts, deletes, requirements, comparison, reader, allocater, onSplit, onJoin)
      
      done <- tx.commit()
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
      kvos1 <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, key0)
      kvos2 <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, key4)
     
    } yield {
      kvos1.pointer should be (l0)
      kvos1.contents.size should be (3)
      kvos1.contents.get(key0) match {
        case None => fail("missing key")
        case Some(v) => v.value should be (value)
      }
      kvos1.contents.get(key1) match {
        case None => fail("missing key")
        case Some(v) => v.value should be (value)
      }
      kvos2.contents.get(key3) match {
        case None => fail("missing key")
        case Some(v) => v.value should be (value)
      }
      kvos1.maximum.isDefined should be(true)
      kvos1.maximum.get.key should be (key3)
      split shouldNot be (null)
      kvos2.pointer shouldNot be (l0)
      kvos2.contents.size should be (2)
      kvos2.contents.get(key3) match {
        case None => fail("missing key")
        case Some(v) => v.value should be (value)
      }
      kvos2.contents.get(key4) match {
        case None => fail("missing key")
        case Some(v) => v.value should be (value)
      }
      kvos2.maximum.isDefined should be (true)
      kvos2.maximum.get.key should be (max0)
    }
  }
 
  test("Test destroy three-node list") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](9))
    
    var preps: List[UUID] = Nil
    
    def prepareForDeletion(kvos: KeyValueObjectState): Future[Unit] = {
      preps = kvos.pointer.uuid :: preps
      Future.unit
    }
    
    for {
      l2 <- alloc(Some(max1), None, None)
      l1 <- alloc(Some(max0), Some(max1), Some(l2))
      l0 <- alloc(None, Some(max0), Some(l1))
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
      kvos <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, target)
      
      toast <- KeyValueList.destroy(sys, lptr, prepareForDeletion)
      
      f2 <- sys.readObject(l2).failed
      f1 <- sys.readObject(l1).failed
      f0 <- sys.readObject(l0).failed
     
    } yield {
      kvos.pointer should be (l1)
      preps.toSet should be (Set(l2.uuid, l1.uuid, l0.uuid))
    }
  }
  
  test("Test destroy two-node list") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](9))
    
    var preps: List[UUID] = Nil
    
    def prepareForDeletion(kvos: KeyValueObjectState): Future[Unit] = {
      preps = kvos.pointer.uuid :: preps
      Future.unit
    }
    
    for {
      l1 <- alloc(Some(max0), None, None)
      l0 <- alloc(None, Some(max0), Some(l1))
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
      kvos <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, target)
      
      toast <- KeyValueList.destroy(sys, lptr, prepareForDeletion)
      
      f1 <- sys.readObject(l1).failed
      f0 <- sys.readObject(l0).failed
     
    } yield {
      kvos.pointer should be (l1)
      preps.toSet should be (Set(l1.uuid, l0.uuid))
    }
  }
  
  test("Test destroy one-node list") {
  
    val max0 = Key(Array[Byte](5))
    val max1 = Key(Array[Byte](10))
    val target = Key(Array[Byte](9))
    
    var preps: List[UUID] = Nil
    
    def prepareForDeletion(kvos: KeyValueObjectState): Future[Unit] = {
      preps = kvos.pointer.uuid :: preps
      Future.unit
    }
    
    for {
      
      l0 <- alloc(None, None, None)
      
      lptr = KeyValueListPointer(Key.AbsoluteMinimum, l0)
      
      kvos <- KeyValueList.fetchContainingNode(sys, lptr, ByteArrayKeyOrdering, target)
      
      toast <- KeyValueList.destroy(sys, lptr, prepareForDeletion)

      f0 <- sys.readObject(l0).failed
     
    } yield {
      kvos.pointer should be (l0)
      preps.toSet should be (Set(l0.uuid))
    }
  }
}