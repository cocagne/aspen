package com.ibm.aspen.base.tieredlist

import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest._
import com.ibm.aspen.base.TestSystem

import com.ibm.aspen.core.HLCTimestamp
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.base.impl.Bootstrap
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue.Insert
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.core.objects.keyvalue.KeyOrdering
import com.ibm.aspen.core.objects.keyvalue.ByteArrayKeyOrdering
import com.ibm.aspen.base.TestSystemSuite
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.SetMin
import com.ibm.aspen.core.objects.keyvalue.SetMax
import com.ibm.aspen.core.objects.keyvalue.SetLeft
import com.ibm.aspen.core.objects.keyvalue.SetRight
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.base.TransactionAborted

class BasicKeyValueSuite extends TestSystemSuite {
  import Bootstrap._
  
  test("Test keyvalue object creation and read") {

    val minimum = Key(List[Byte](1,2).toArray)
    val maximum = Key(List[Byte](3,4).toArray)
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    
    val t = HLCTimestamp(5)

    implicit val tx = sys.newTransaction()
    
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
          SetMin(minimum) :: SetMax(maximum) :: SetLeft(left) :: SetRight(right) :: Insert(k1,k1,t) :: Insert(k2,k2,t) :: Nil, 
          None)
          
      done <- tx.commit()
      
      kvos <- sys.readObject(kvp)
      
    } yield {
      kvos.refcount should be (ObjectRefcount(0,1))
      kvos.minimum.isDefined should be (true)
      kvos.minimum.get should be (minimum)
      kvos.maximum.isDefined should be (true)
      kvos.maximum.get should be (maximum)
      kvos.left.isDefined should be (true)
      kvos.left.get should be (left)
      kvos.right.isDefined should be (true)
      kvos.right.get should be (right)
      kvos.contents.size should be (2)
      kvos.contents.get(Key(k1)) match {
        case None => fail("missin k1")
        case Some(v) => v.value should be (k1)
      }
      kvos.contents.get(Key(k2)) match {
        case None => fail("missin k2")
        case Some(v) => v.value should be (k2)
      }
    }
  }
  
  test("Test keyvalue object creation, update, and read") {
    
    val minimum = Key(List[Byte](1,2).toArray)
    val maximum = Key(List[Byte](3,4).toArray)
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    
    val t = HLCTimestamp(5)

    val tx1 = sys.newTransaction()
    
    for {
      r <- sys.readObject(sys.radiclePointer)
      
      // give transaction something to do
      meh = tx1.bumpVersion(sys.radiclePointer, r.revision)
      
      kvp <- sys.lowLevelAllocateKeyValueObject(
          sys.radiclePointer, 
          ObjectRevision(UUID.randomUUID()), 
          BootstrapStoragePoolUUID, 
          None,
          TestSystem.DefaultIDA, 
          SetMin(minimum) :: SetMax(maximum) :: SetLeft(left) :: SetRight(right) :: Insert(k1,k1,t) :: Nil, 
          None)(tx1, executionContext)
          
      done <- tx1.commit()
      
      tx2 = sys.newTransaction()
      
      _ = tx2.append(kvp, None, List(KeyValueUpdate.KVRequirement(Key(k2), t, KeyValueUpdate.TimestampRequirement.DoesNotExist)), List(new Insert(k2,k2,t)))
      
      done2 <- tx2.commit()
      
      kvos <- sys.readObject(kvp)
      
    } yield {
      kvos.minimum.isDefined should be (true)
      kvos.minimum.get should be (minimum)
      kvos.maximum.isDefined should be (true)
      kvos.maximum.get should be (maximum)
      kvos.left.isDefined should be (true)
      kvos.left.get should be (left)
      kvos.right.isDefined should be (true)
      kvos.right.get should be (right)
      kvos.contents.size should be (2)
      kvos.contents.get(Key(k1)) match {
        case None => fail("missin k1")
        case Some(v) => v.value should be (k1)
      }
      kvos.contents.get(Key(k2)) match {
        case None => fail("missin k2")
        case Some(v) => v.value should be (k2)
      }
    }
  }
  
  test("Test keyvalue object overwrite fails on version mismatch") {
    
    val minimum = Key(List[Byte](1,2).toArray)
    val maximum = Key(List[Byte](3,4).toArray)
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    
    val t = HLCTimestamp(5)

    val tx1 = sys.newTransaction()
    
    for {
      r <- sys.readObject(sys.radiclePointer)
      
      // give transaction something to do
      meh = tx1.bumpVersion(sys.radiclePointer, r.revision)
      
      kvp <- sys.lowLevelAllocateKeyValueObject(
          sys.radiclePointer, 
          ObjectRevision(UUID.randomUUID()), 
          BootstrapStoragePoolUUID, 
          None,
          TestSystem.DefaultIDA, 
          SetMin(minimum) :: SetMax(maximum) :: SetLeft(left) :: SetRight(right) :: Insert(k1,k1,t) :: Nil, 
          None)(tx1, executionContext)
          
      done <- tx1.commit()
      
      tx2 = sys.newTransaction()
      
      _ = tx2.overwrite(kvp, ObjectRevision(new UUID(1,1)), Nil, Nil)
      
      err <- tx2.commit().failed

    } yield {
      err.isInstanceOf[TransactionAborted] should be (true)
    }
  }
  
  test("Test single-key read, success") {
    
    val minimum = Key(List[Byte](1,2).toArray)
    val maximum = Key(List[Byte](3,4).toArray)
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    
    val t = HLCTimestamp(5)

    implicit val tx = sys.newTransaction()
    
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
          SetMin(minimum) :: SetMax(maximum) :: SetLeft(left) :: SetRight(right) :: Insert(k1,k1,t) :: Insert(k2,k2,t) :: Nil,
          None)
          
      done <- tx.commit()
      
      kvos <- sys.readSingleKey(kvp, Key(k2), ByteArrayKeyOrdering)
      
    } yield {
      kvos.minimum.isDefined should be (false)
      kvos.maximum.isDefined should be (false)
      kvos.left.isDefined should be (false)
      kvos.right.isDefined should be (false)
      
      kvos.contents.size should be (1)
      
      kvos.contents.get(Key(k2)) match {
        case None => fail("missin k2")
        case Some(v) => v.value should be (k2)
      }
    }
  }
  
  test("Test single-key read, out of bounds") {
    
    val minimum = Key(List[Byte](0).toArray)
    val maximum = Key(List[Byte](7).toArray)
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    val k3 = List[Byte](8).toArray
    
    val t = HLCTimestamp(5)

    implicit val tx = sys.newTransaction()
    
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
          SetMin(minimum) :: SetMax(maximum) :: SetLeft(left) :: SetRight(right) :: Insert(k1,k1,t) :: Insert(k2,k2,t) :: Nil,
          None)
          
      done <- tx.commit()
      
      kvos <- sys.readSingleKey(kvp, Key(k3), ByteArrayKeyOrdering)
      
    } yield {
      kvos.minimum.isDefined should be (true)
      kvos.minimum.get should be (minimum)
      kvos.maximum.isDefined should be (true)
      kvos.maximum.get should be (maximum)
      kvos.left.isDefined should be (true)
      kvos.left.get should be (left)
      kvos.right.isDefined should be (true)
      kvos.right.get should be (right)
      kvos.contents.size should be (0)
    }
  }
  
  test("Test single-key read, in bounds no key") {
    
    val minimum = Key(List[Byte](0).toArray)
    val maximum = Key(List[Byte](9).toArray)
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    val k3 = List[Byte](8).toArray
    
    val t = HLCTimestamp(5)

    implicit val tx = sys.newTransaction()
    
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
          SetMin(minimum) :: SetMax(maximum) :: SetLeft(left) :: SetRight(right) :: Insert(k1,k1,t) :: Insert(k2,k2,t) :: Nil,
          None)
          
      done <- tx.commit()
      
      kvos <- sys.readSingleKey(kvp, Key(k3), ByteArrayKeyOrdering)
      
    } yield {
      kvos.minimum.isDefined should be (false)
      kvos.maximum.isDefined should be (false)
      kvos.left.isDefined should be (false)
      kvos.right.isDefined should be (false)
      kvos.contents.size should be (0)
    }
  }
  
  test("Test LargetKeyLessThan read, success") {
    
    val minimum = Key(List[Byte](0).toArray)
    val maximum = Key(List[Byte](9).toArray)
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](4).toArray
    val kt = List[Byte](6).toArray
    val k3 = List[Byte](8).toArray
    
    val t = HLCTimestamp(5)

    implicit val tx = sys.newTransaction()
    
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
          SetMin(minimum) :: SetMax(maximum) :: SetLeft(left) :: SetRight(right) :: Insert(k1,k1,t) :: Insert(k2,k2,t) :: Insert(k3,k3,t) :: Nil,
          None)
          
      done <- tx.commit()
      
      kvos <- sys.readLargestKeyLessThan(kvp, Key(kt), ByteArrayKeyOrdering)
      
    } yield {
      kvos.minimum.isDefined should be (false)
      kvos.maximum.isDefined should be (false)
      kvos.left.isDefined should be (false)
      kvos.right.isDefined should be (false)
      
      kvos.contents.size should be (1)
      
      kvos.contents.get(Key(k2)) match {
        case None => fail("missin k2")
        case Some(v) => v.value should be (k2)
      }
    }
  }
  
  test("Test LargetKeyLessThan read, no matching key") {
    
    val minimum = Key(List[Byte](0).toArray)
    val maximum = Key(List[Byte](9).toArray)
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val kt = List[Byte](2).toArray
    val k1 = List[Byte](3).toArray
    val k2 = List[Byte](4).toArray
    val k3 = List[Byte](8).toArray
    
    val t = HLCTimestamp(5)

    implicit val tx = sys.newTransaction()
    
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
          SetMin(minimum) :: SetMax(maximum) :: SetLeft(left) :: SetRight(right) :: Insert(k1,k1,t) :: Insert(k2,k2,t) :: Insert(k3,k3,t) :: Nil, 
          None)
          
      done <- tx.commit()
      
      kvos <- sys.readLargestKeyLessThan(kvp, Key(kt), ByteArrayKeyOrdering)
      
    } yield {
      kvos.minimum.isDefined should be (true)
      kvos.minimum.get should be (minimum)
      kvos.maximum.isDefined should be (true)
      kvos.maximum.get should be (maximum)
      kvos.left.isDefined should be (true)
      kvos.left.get should be (left)
      kvos.right.isDefined should be (true)
      kvos.right.get should be (right)
      kvos.contents.size should be (0)
    }
  }
  
  test("Test KeyRange reads") {
    
    val minimum = Key(List[Byte](0).toArray)
    val maximum = Key(List[Byte](9).toArray)
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
   
    val k1 = List[Byte](3).toArray
    val k2 = List[Byte](5).toArray
    val k3 = List[Byte](7).toArray
    val k4 = List[Byte](9).toArray
    
    val kprepre = Key(List[Byte](0).toArray)
    val kpre    = Key(List[Byte](1).toArray)
    val k1x2    = Key(List[Byte](4).toArray)
    val k3x4    = Key(List[Byte](8).toArray)
    val kpost   = Key(List[Byte](11).toArray)
    val kpost2  = Key(List[Byte](15).toArray)
    
    val t = HLCTimestamp(5)

    val ops = SetMin(minimum) :: SetMax(maximum) :: SetLeft(left) :: SetRight(right) :: 
              Insert(k1,k1,t) :: Insert(k2,k2,t) :: Insert(k3,k3,t) :: Insert(k4,k4,t) :: Nil

    implicit val tx = sys.newTransaction()
    
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
      
      kvos1 <- sys.readKeyRange(kvp, kprepre, kpre, ByteArrayKeyOrdering)
      kvos2 <- sys.readKeyRange(kvp, kpre, Key(k1), ByteArrayKeyOrdering)
      kvos3 <- sys.readKeyRange(kvp, k1x2, k3x4, ByteArrayKeyOrdering)
      kvos4 <- sys.readKeyRange(kvp, k3x4, kpost, ByteArrayKeyOrdering)
      kvos5 <- sys.readKeyRange(kvp, kpost, kpost2, ByteArrayKeyOrdering)
      
    } yield {
      kvos1.minimum.isDefined should be (true)
      kvos1.minimum.get should be (minimum)
      kvos1.maximum.isDefined should be (true)
      kvos1.maximum.get should be (maximum)
      kvos1.left.isDefined should be (true)
      kvos1.left.get should be (left)
      kvos1.right.isDefined should be (true)
      kvos1.right.get should be (right)
      kvos1.contents.size should be (0)
      
      kvos2.contents.size should be (1)
      kvos2.contents.get(Key(k1)) match {
        case None => fail("missin k1")
        case Some(v) => v.value should be (k1)
      }
      
      kvos3.contents.size should be (2)
      kvos3.contents.get(Key(k2)) match {
        case None => fail("missin k2")
        case Some(v) => v.value should be (k2)
      }
      kvos3.contents.get(Key(k3)) match {
        case None => fail("missin k3")
        case Some(v) => v.value should be (k3)
      }
      
      kvos4.contents.size should be (1)
      kvos4.contents.get(Key(k4)) match {
        case None => fail("missin k4")
        case Some(v) => v.value should be (k4)
      }
      
      kvos5.contents.size should be (0)
    }
  }
  
  test("KeyValueListPointer Encoding") {
    val objUUID = new UUID(3,4)
    val poolUUID = new UUID(5,6)
    val size = Some(10)
    val p1 = StorePointer(0, new Array[Byte](0))
    val p2 = StorePointer(1, new Array[Byte](0))
    val p3 = StorePointer(2, new Array[Byte](0))
    val pointers = (p1::p2::p3::Nil).toArray
    val ida = Replication(3,2)
    
    val op = KeyValueObjectPointer(objUUID, poolUUID, size, ida, pointers)
    val min = Array[Byte](0,1,2,3,4)
    
    val kvlp = KeyValueListPointer(Key(min), op)
    
    val arr = kvlp.toArray
    
    val kvlp2 = KeyValueListPointer.fromArray(arr)
    
    kvlp2 should be (kvlp)
  }
  
  test("KeyValueListPointer Encoding - Empty Minimum Array") {
    val objUUID = new UUID(3,4)
    val poolUUID = new UUID(5,6)
    val size = Some(10)
    val p1 = StorePointer(0, new Array[Byte](0))
    val p2 = StorePointer(1, new Array[Byte](0))
    val p3 = StorePointer(2, new Array[Byte](0))
    val pointers = (p1::p2::p3::Nil).toArray
    val ida = Replication(3,2)
    
    val op = KeyValueObjectPointer(objUUID, poolUUID, size, ida, pointers)
    val min = new Array[Byte](0)
    
    val kvlp = KeyValueListPointer(Key(min), op)
    
    val arr = kvlp.toArray
    
    val kvlp2 = KeyValueListPointer.fromArray(arr)
    
    kvlp2 should be (kvlp)
  }
}