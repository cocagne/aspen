package com.ibm.aspen.base.keyvalue

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
import com.ibm.aspen.core.objects.keyvalue.KeyComparison
import com.ibm.aspen.core.objects.keyvalue.KeyComparison
import com.ibm.aspen.core.objects.keyvalue.ByteArrayComparison

class BasicKeyValueSuite extends AsyncFunSuite with Matchers {
  import Bootstrap._
  
  test("Test keyvalue object creation and read") {
    
    val ts = new TestSystem()
    val sys = ts.aspenSystem
    
    val minimum = List[Byte](1,2).toArray
    val maximum = List[Byte](3,4).toArray
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    
    val t = HLCTimestamp(5)
    
    val m = Map[Array[Byte],Array[Byte]]( (k1->k1), (k2->k2) )

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
          m, 
          Some(minimum), Some(maximum), Some(left), Some(right), None)
          
      done <- tx.commit()
      
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
  
  test("Test keyvalue object creation, update, and read") {
    
    val ts = new TestSystem()
    val sys = ts.aspenSystem
    
    val minimum = List[Byte](1,2).toArray
    val maximum = List[Byte](3,4).toArray
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    
    val t = HLCTimestamp(5)
    
    val m = Map[Array[Byte],Array[Byte]]( (k1->k1) )

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
          m, 
          Some(minimum), Some(maximum), Some(left), Some(right), None)(tx1, executionContext)
          
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
  
  test("Test single-key read, success") {
    
    val ts = new TestSystem()
    val sys = ts.aspenSystem
    
    val minimum = List[Byte](1,2).toArray
    val maximum = List[Byte](3,4).toArray
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    
    val t = HLCTimestamp(5)
    
    val m = Map[Array[Byte],Array[Byte]]( (k1->k1), (k2->k2) )

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
          m, 
          Some(minimum), Some(maximum), Some(left), Some(right), None)
          
      done <- tx.commit()
      
      kvos <- sys.readSingleKey(kvp, Key(k2), new ByteArrayComparison())
      
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
    
    val ts = new TestSystem()
    val sys = ts.aspenSystem
    
    val minimum = List[Byte](0).toArray
    val maximum = List[Byte](7).toArray
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    val k3 = List[Byte](8).toArray
    
    val t = HLCTimestamp(5)
    
    val m = Map[Array[Byte],Array[Byte]]( (k1->k1), (k2->k2) )

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
          m, 
          Some(minimum), Some(maximum), Some(left), Some(right), None)
          
      done <- tx.commit()
      
      kvos <- sys.readSingleKey(kvp, Key(k3), new ByteArrayComparison())
      
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
    
    val ts = new TestSystem()
    val sys = ts.aspenSystem
    
    val minimum = List[Byte](0).toArray
    val maximum = List[Byte](9).toArray
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](2).toArray
    val k3 = List[Byte](8).toArray
    
    val t = HLCTimestamp(5)
    
    val m = Map[Array[Byte],Array[Byte]]( (k1->k1), (k2->k2) )

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
          m, 
          Some(minimum), Some(maximum), Some(left), Some(right), None)
          
      done <- tx.commit()
      
      kvos <- sys.readSingleKey(kvp, Key(k3), new ByteArrayComparison())
      
    } yield {
      kvos.minimum.isDefined should be (false)
      kvos.maximum.isDefined should be (false)
      kvos.left.isDefined should be (false)
      kvos.right.isDefined should be (false)
      kvos.contents.size should be (0)
    }
  }
  
  test("Test LargetKeyLessThan read, success") {
    
    val ts = new TestSystem()
    val sys = ts.aspenSystem
    
    val minimum = List[Byte](0).toArray
    val maximum = List[Byte](9).toArray
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val k1 = List[Byte](1).toArray
    val k2 = List[Byte](4).toArray
    val kt = List[Byte](6).toArray
    val k3 = List[Byte](8).toArray
    
    val t = HLCTimestamp(5)
    
    val m = Map[Array[Byte],Array[Byte]]( (k1->k1), (k2->k2), (k3->k3) )

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
          m, 
          Some(minimum), Some(maximum), Some(left), Some(right), None)
          
      done <- tx.commit()
      
      kvos <- sys.readLargestKeyLessThan(kvp, Key(kt), new ByteArrayComparison())
      
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
    
    val ts = new TestSystem()
    val sys = ts.aspenSystem
    
    val minimum = List[Byte](0).toArray
    val maximum = List[Byte](9).toArray
    val right   = List[Byte](5,6).toArray
    val left    = List[Byte](7,8,9).toArray
    
    val kt = List[Byte](2).toArray
    val k1 = List[Byte](3).toArray
    val k2 = List[Byte](4).toArray
    val k3 = List[Byte](8).toArray
    
    val t = HLCTimestamp(5)
    
    val m = Map[Array[Byte],Array[Byte]]( (k1->k1), (k2->k2), (k3->k3) )

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
          m, 
          Some(minimum), Some(maximum), Some(left), Some(right), None)
          
      done <- tx.commit()
      
      kvos <- sys.readLargestKeyLessThan(kvp, Key(kt), new ByteArrayComparison())
      
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
    
    val ts = new TestSystem()
    val sys = ts.aspenSystem
    
    val minimum = List[Byte](0).toArray
    val maximum = List[Byte](9).toArray
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
    
    val m = Map[Array[Byte],Array[Byte]]( (k1->k1), (k2->k2), (k3->k3), (k4->k4) )

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
          m, 
          Some(minimum), Some(maximum), Some(left), Some(right), None)
          
      done <- tx.commit()
      
      kvos1 <- sys.readKeyRange(kvp, kprepre, kpre, new ByteArrayComparison())
      kvos2 <- sys.readKeyRange(kvp, kpre, Key(k1), new ByteArrayComparison())
      kvos3 <- sys.readKeyRange(kvp, k1x2, k3x4, new ByteArrayComparison())
      kvos4 <- sys.readKeyRange(kvp, k3x4, kpost, new ByteArrayComparison())
      kvos5 <- sys.readKeyRange(kvp, kpost, kpost2, new ByteArrayComparison())
      
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
}