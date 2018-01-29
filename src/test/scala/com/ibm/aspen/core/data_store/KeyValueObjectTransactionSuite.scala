package com.ibm.aspen.core.data_store

import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest._
import java.util.UUID
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.transaction.TransactionRequirement
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.DataBuffer
import java.nio.ByteBuffer
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.allocation.KeyValueAllocationOptions
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectStoreState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue._
import com.ibm.aspen.util._

object KeyValueObjectTransactionSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
  val uuid0 = new UUID(0,0)
  val uuid1 = new UUID(0,1)
  val uuid2 = new UUID(0,4)
  val poolUUID = new UUID(0,2)
  val txUUID = new UUID(0,3)
  val txUUID2 = new UUID(1,3)
  val txUUID3 = new UUID(2,3)
  val allocUUID = new UUID(0,4)
  
  val ida = Replication(3,2)
  
  val allocObj = KeyValueObjectPointer(new UUID(0,4), poolUUID, None, ida, new Array[StorePointer](0))
  val allocRev = ObjectRevision.Null
  val oneRef = ObjectRefcount(0,1)
  
  val initialTimestamp = HLCTimestamp.now
  
  def mkObjPtr(objUUID:UUID, sp:StorePointer) = KeyValueObjectPointer(objUUID, poolUUID, None, ida, (sp::Nil).toArray)
  
  def mktxd(reqs: List[TransactionRequirement], txdUUID:UUID=txUUID) = {
    TransactionDescription(txdUUID, 100, allocObj, 0, reqs, Nil)
  }
  
  def lu(op: ObjectPointer, db: DataBuffer): Option[List[LocalUpdate]] = Some(List(LocalUpdate(op.uuid, db)))
  
  val storeId = DataStoreID(poolUUID, 1)
  
  val irev = ObjectRevision(allocUUID)
  
  def oeq(a: Option[Array[Byte]], b: Option[Array[Byte]]): Boolean = {
    //println(s"Comparing $a to $b")
    (a,b) match {
      case (None,None) => true
      case (Some(_), None) => false
      case (None, Some(_)) => false
      case (Some(aa), Some(bb)) =>
        //println(s"aa ${arr2string(aa)} bb ${arr2string(bb)}")
        java.util.Arrays.equals(aa, bb)
    }
  }
  
  case class State(md: ObjectMetadata, kv: KeyValueObjectStoreState, locks: Set[Lock])
  
  def k(i: Byte): Key = Key(List[Byte](i).toArray)
}

class KeyValueObjectTransactionSuite extends AsyncFunSuite with Matchers {
  
  import KeyValueObjectTransactionSuite._
  
  def newStore: DataStore = new DataStoreFrontend(storeId, new MemoryOnlyDataStoreBackend()(ExecutionContext.Implicits.global), Nil, Nil)
  
  // Helper method that creates a store and adds two objects. Returns (DataStore, Obj0StorePointer, Obj1StorePointer)
  def initObjects(): (DataStore, KeyValueObjectPointer, KeyValueObjectPointer) = {
    val ds = newStore

    implicit val executionContext = ExecutionContext.Implicits.global
    
    val lno0 = List(Allocate.NewObject(uuid0, new KeyValueAllocationOptions, None, oneRef, DataBuffer.Empty))
    val lno1 = List(Allocate.NewObject(uuid1, new KeyValueAllocationOptions, None, oneRef, DataBuffer.Empty))
    
    val f = ds.allocate(lno0, initialTimestamp, allocUUID, allocObj, allocRev) flatMap { either => either match {
      case Right(ars0) => ds.allocate(lno1, initialTimestamp, allocUUID, allocObj, allocRev).flatMap(er => er match {
        case Right(ars1) => 
          val op0 = mkObjPtr(uuid0, ars0.newObjects.head.storePointer)
          val op1 = mkObjPtr(uuid1, ars1.newObjects.head.storePointer)
          Future.successful((ds, op0, op1))
        case Left(err) => fail("Returned failure instead of object content")
      })
      case Left(err) => fail("Returned failure instead of store pointer")
    }}
    
    Await.result(f, awaitDuration)
  }
  
  def get(ds: DataStore, pointer: ObjectPointer): State = {
    val r = Await.result(ds.getObject(pointer), awaitDuration)
    r match {      
      case Left(_) => fail
      case Right((md, data, locks)) => State(md, KeyValueObjectStoreState(0, data), locks.toSet)
    }
  }
  
  test("Allocate & Load Empty Key Value Object") {
    val (ds, op0, op1) = initObjects()
    
    val s0 = get(ds, op0)
    val s1 = get(ds, op1)
    
    s0.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s1.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s0.kv.minimum.isEmpty && s1.kv.minimum.isEmpty && s0.kv.maximum.isEmpty && s1.kv.maximum.isEmpty should be (true)
    s0.kv.idaEncodedLeft.isEmpty && s1.kv.idaEncodedLeft.isEmpty should be (true)
    s0.kv.idaEncodedRight.isEmpty && s1.kv.idaEncodedRight.isEmpty should be (true)
    s0.kv.idaEncodedContents.isEmpty && s1.kv.idaEncodedContents.isEmpty should be (true)
    s0.locks.isEmpty && s1.locks.isEmpty should be (true)
  }
  
  test("Set Optional Arrays") {
    val (ds, op0, op1) = initObjects()
    
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Overwrite, Some(irev), Nil, initialTimestamp) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    val aleft = List[Byte](1,2).toArray
    val aright = List[Byte](3,4).toArray
    val amin = List[Byte](5,6).toArray
    val amax = List[Byte](5,6).toArray
    
    val ops = List(new SetLeft(aleft), new SetRight(aright), new SetMin(amin), new SetMax(amax)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val updates = lu(op0, db)
    
    val errs = Await.result(ds.lockTransaction(txd, updates), awaitDuration)
    
    errs should be (Nil)
    
    val s0 = get(ds, op0)
    val s1 = get(ds, op1)
    
    s0.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s1.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s0.kv.minimum.isEmpty && s1.kv.minimum.isEmpty && s0.kv.maximum.isEmpty && s1.kv.maximum.isEmpty should be (true)
    s0.kv.idaEncodedLeft.isEmpty && s1.kv.idaEncodedLeft.isEmpty should be (true)
    s0.kv.idaEncodedRight.isEmpty && s1.kv.idaEncodedRight.isEmpty should be (true)
    s0.kv.idaEncodedContents.isEmpty && s1.kv.idaEncodedContents.isEmpty should be (true)
    s0.locks should be (Set(RevisionWriteLock(txd))) 
    s1.locks should be (Set())
    
    Await.result(ds.commitTransactionUpdates(txd, updates), awaitDuration)
    
    val a0 = get(ds, op0)
    val a1 = get(ds, op1)
    
    a0.md should be (ObjectMetadata(ObjectRevision(txd.transactionUUID), ObjectRefcount(0,1), txdts))
    a1.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    
    oeq(a0.kv.minimum, Some(amin)) should be (true)
    oeq(a0.kv.maximum, Some(amax)) should be (true)
    a1.kv.minimum.isEmpty && a1.kv.maximum.isEmpty should be (true)
    
    oeq(a0.kv.idaEncodedLeft, Some(aleft)) should be (true)
    oeq(a0.kv.idaEncodedRight, Some(aright)) should be (true)
    
    a1.kv.idaEncodedLeft.isEmpty should be (true)
    a1.kv.idaEncodedRight.isEmpty should be (true)
    a0.kv.idaEncodedContents.isEmpty && s1.kv.idaEncodedContents.isEmpty should be (true)
    a0.locks should be (Set()) 
    a1.locks should be (Set())
  }
  
  test("Set single key with no revision change") {
    val (ds, op0, op1) = initObjects()
    
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, Nil, initialTimestamp) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    val key = k(2)
    val value = List[Byte](3,4).toArray
    val valuets = HLCTimestamp(10)
    
    val ops = List(new Insert(key.bytes, value, valuets)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val updates = lu(op0, db)
    
    val errs = Await.result(ds.lockTransaction(txd, updates), awaitDuration)
    
    errs should be (Nil)
    
    Await.result(ds.commitTransactionUpdates(txd, updates), awaitDuration)
    
    val s0 = get(ds, op0)
    val s1 = get(ds, op1)
    
    s0.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s1.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s0.kv.minimum.isEmpty && s1.kv.minimum.isEmpty && s0.kv.maximum.isEmpty && s1.kv.maximum.isEmpty should be (true)
    s0.kv.idaEncodedLeft.isEmpty && s1.kv.idaEncodedLeft.isEmpty should be (true)
    s0.kv.idaEncodedRight.isEmpty && s1.kv.idaEncodedRight.isEmpty should be (true)
    s0.kv.idaEncodedContents.isEmpty should be (false) 
    s0.kv.idaEncodedContents.contains(key) should be (true)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key).value, value) should be (true)
    s0.kv.idaEncodedContents(key).timestamp should be (valuets)
    s1.kv.idaEncodedContents.isEmpty should be (true)
    s0.locks should be (Set()) 
    s1.locks should be (Set()) 
  }
  
  test("Set single key with no revision change, unlocked commit") {
    val (ds, op0, op1) = initObjects()
    
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, Nil, initialTimestamp) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    val key = k(2)
    val value = List[Byte](3,4).toArray
    val valuets = HLCTimestamp(10)
    
    val ops = List(new Insert(key.bytes, value, valuets)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val updates = lu(op0, db)
    
    Await.result(ds.commitTransactionUpdates(txd, updates), awaitDuration)
    
    val s0 = get(ds, op0)
    val s1 = get(ds, op1)
    
    s0.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s1.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s0.kv.minimum.isEmpty && s1.kv.minimum.isEmpty && s0.kv.maximum.isEmpty && s1.kv.maximum.isEmpty should be (true)
    s0.kv.idaEncodedLeft.isEmpty && s1.kv.idaEncodedLeft.isEmpty should be (true)
    s0.kv.idaEncodedRight.isEmpty && s1.kv.idaEncodedRight.isEmpty should be (true)
    s0.kv.idaEncodedContents.isEmpty should be (false) 
    s0.kv.idaEncodedContents.contains(key) should be (true)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key).value, value) should be (true)
    s0.kv.idaEncodedContents(key).timestamp should be (valuets)
    s1.kv.idaEncodedContents.isEmpty should be (true)
    s0.locks should be (Set()) 
    s1.locks should be (Set()) 
  }
  
  test("Set multiple keys with no revision change") {
    val (ds, op0, op1) = initObjects()
    
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, Nil, initialTimestamp) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    val key1 = k(2)
    val key2 = k(3)
    val value = List[Byte](3,4).toArray
    val valuets = HLCTimestamp(10)
    
    val ops = List(new Insert(key1.bytes, value, valuets), new Insert(key2.bytes, value, valuets)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val updates = lu(op0, db)
    
    val errs = Await.result(ds.lockTransaction(txd, updates), awaitDuration)
    
    errs should be (Nil)
    
    Await.result(ds.commitTransactionUpdates(txd, updates), awaitDuration)
    
    val s0 = get(ds, op0)
    val s1 = get(ds, op1)
    
    s0.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s1.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s0.kv.minimum.isEmpty && s1.kv.minimum.isEmpty && s0.kv.maximum.isEmpty && s1.kv.maximum.isEmpty should be (true)
    s0.kv.idaEncodedLeft.isEmpty && s1.kv.idaEncodedLeft.isEmpty should be (true)
    s0.kv.idaEncodedRight.isEmpty && s1.kv.idaEncodedRight.isEmpty should be (true)
    s0.kv.idaEncodedContents.isEmpty should be (false) 
    s0.kv.idaEncodedContents.contains(key1) should be (true)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key1).value, value) should be (true)
    s0.kv.idaEncodedContents(key1).timestamp should be (valuets)
    s0.kv.idaEncodedContents.contains(key2) should be (true)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key2).value, value) should be (true)
    s0.kv.idaEncodedContents(key2).timestamp should be (valuets)
    s1.kv.idaEncodedContents.isEmpty should be (true)
    s0.locks should be (Set()) 
    s1.locks should be (Set())
  }
  
  test("Set multiple transactions setting and overwriting keys with overwrite transaction") {
    val (ds, op0, op1) = initObjects()
    
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Overwrite, Some(irev), Nil, initialTimestamp) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    val key1 = k(2)
    val key2 = k(3)
    val key3 = k(4)
    val value = List[Byte](3,4).toArray
    val value2 = List[Byte](5,6).toArray
    val valuets = HLCTimestamp(10)
    val valuets2 = HLCTimestamp(15)
    
    val ops = List(new Insert(key1.bytes, value, valuets), new Insert(key2.bytes, value, valuets)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val updates = lu(op0, db)
    
    val errs = Await.result(ds.lockTransaction(txd, updates), awaitDuration)
    
    errs should be (Nil)
    
    Await.result(ds.commitTransactionUpdates(txd, updates), awaitDuration)
    
    var s0 = get(ds, op0)
    
    s0.md should be (ObjectMetadata(ObjectRevision(txd.transactionUUID), ObjectRefcount(0,1), txdts))
    s0.kv.idaEncodedContents.size should be (2)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key1).value, value) should be (true)
    s0.kv.idaEncodedContents(key1).timestamp should be (valuets)
    s0.kv.idaEncodedContents.contains(key2) should be (true)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key2).value, value) should be (true)
    s0.kv.idaEncodedContents(key2).timestamp should be (valuets)
    s0.locks should be (Set())
    
    
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Overwrite, Some(ObjectRevision(txd.transactionUUID)), Nil, txdts) :: Nil, txUUID2)
                    
    val txdts2 = HLCTimestamp(txd2.startTimestamp)
    
    val ops2 = List(new Delete(key1.bytes), new Insert(key2.bytes, value2, valuets2), new Insert(key3.bytes, value, valuets2))
    val db2 = KeyValueObjectCodec.encodeUpdate(ida, ops2)(0)
    
    val updates2 = lu(op0, db2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, updates2), awaitDuration)
    
    errs2 should be (Nil)
    
    Await.result(ds.commitTransactionUpdates(txd2, updates2), awaitDuration)
    
    s0 = get(ds, op0)
    
    s0.md should be (ObjectMetadata(ObjectRevision(txd2.transactionUUID), ObjectRefcount(0,1), txdts2))
    s0.kv.idaEncodedContents.size should be (2)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key2).value, value2) should be (true)
    s0.kv.idaEncodedContents(key2).timestamp should be (valuets2)
    s0.kv.idaEncodedContents.contains(key3) should be (true)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key3).value, value) should be (true)
    s0.kv.idaEncodedContents(key2).timestamp should be (valuets2)
    s0.locks should be (Set())
  }
  
  test("Set multiple transactions setting and overwriting keys with append transaction") {
    val (ds, op0, op1) = initObjects()
    
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Overwrite, Some(irev), Nil, initialTimestamp) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    val key1 = k(2)
    val key2 = k(3)
    val key3 = k(4)
    val value = List[Byte](3,4).toArray
    val value2 = List[Byte](5,6).toArray
    val valuets = HLCTimestamp(10)
    val valuets2 = HLCTimestamp(15)
    
    val ops = List(new Insert(key1.bytes, value, valuets), new Insert(key2.bytes, value, valuets)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val updates = lu(op0, db)
    
    val errs = Await.result(ds.lockTransaction(txd, updates), awaitDuration)
    
    errs should be (Nil)
    
    Await.result(ds.commitTransactionUpdates(txd, updates), awaitDuration)
    
    var s0 = get(ds, op0)
    
    s0.md should be (ObjectMetadata(ObjectRevision(txd.transactionUUID), ObjectRefcount(0,1), txdts))
    s0.kv.idaEncodedContents.size should be (2)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key1).value, value) should be (true)
    s0.kv.idaEncodedContents(key1).timestamp should be (valuets)
    s0.kv.idaEncodedContents.contains(key2) should be (true)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key2).value, value) should be (true)
    s0.kv.idaEncodedContents(key2).timestamp should be (valuets)
    s0.locks should be (Set())
    
    
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, Nil, txdts) :: Nil, txUUID2)
                    
    val txdts2 = HLCTimestamp(txd2.startTimestamp)
    
    val ops2 = List(new Delete(key1.bytes), new Insert(key2.bytes, value2, valuets2), new Insert(key3.bytes, value, valuets2))
    val db2 = KeyValueObjectCodec.encodeUpdate(ida, ops2)(0)
    
    val updates2 = lu(op0, db2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, updates2), awaitDuration)
    
    errs2 should be (Nil)
    
    Await.result(ds.commitTransactionUpdates(txd2, updates2), awaitDuration)
    
    s0 = get(ds, op0)
    
    s0.md should be (ObjectMetadata(ObjectRevision(txd.transactionUUID), ObjectRefcount(0,1), txdts))
    s0.kv.idaEncodedContents.size should be (2)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key2).value, value2) should be (true)
    s0.kv.idaEncodedContents(key2).timestamp should be (valuets2)
    s0.kv.idaEncodedContents.contains(key3) should be (true)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key3).value, value) should be (true)
    s0.kv.idaEncodedContents(key2).timestamp should be (valuets2)
    s0.locks should be (Set())
  }
  
  
  
  test("Revision lock conflicts") {
    val (ds, op0, op1) = initObjects()
    
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Overwrite, Some(irev), Nil, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(k(2).bytes, List[Byte](3,4).toArray, HLCTimestamp(10))) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val errs = Await.result(ds.lockTransaction(txd, lu(op0, db)), awaitDuration)
    
    errs should be (Nil)
    
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, Some(irev), Nil, HLCTimestamp(10)) :: Nil, txUUID2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, lu(op0, db)), awaitDuration)
    
    errs2.toSet should be (Set(TransactionCollision(op0, txd)))
  }
  
  test("Key lock prevents revision lock") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
        
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(k(2).bytes, List[Byte](3,4).toArray, HLCTimestamp(10))) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val errs = Await.result(ds.lockTransaction(txd, lu(op0, db)), awaitDuration)
    
    errs should be (Nil)
    
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Overwrite, Some(irev), Nil, HLCTimestamp(10)) :: Nil, txUUID2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, lu(op0, db)), awaitDuration)
    
    errs2.toSet should be (Set(TransactionCollision(op0, txd)))
  }
  
  test("Revision lock prevents key lock") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    
        
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Overwrite, Some(irev), Nil, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(k(2).bytes, List[Byte](3,4).toArray, HLCTimestamp(10))) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val errs = Await.result(ds.lockTransaction(txd, lu(op0, db)), awaitDuration)
    
    errs should be (Nil)
    
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
    
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, HLCTimestamp(10)) :: Nil, txUUID2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, lu(op0, db)), awaitDuration)
    
    errs2.toSet should be (Set(TransactionCollision(op0, txd)))
  }
  
  test("Overwrite with key requirements does not conflict with self") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Overwrite, Some(irev), reqs, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(k(2).bytes, List[Byte](3,4).toArray, HLCTimestamp(10))) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val errs = Await.result(ds.lockTransaction(txd, lu(op0, db)), awaitDuration)
    
    errs should be (Nil)
  }
  
  test("Key lock conflicts") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
        
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(k(2).bytes, List[Byte](3,4).toArray, HLCTimestamp(10))) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val errs = Await.result(ds.lockTransaction(txd, lu(op0, db)), awaitDuration)
    
    errs should be (Nil)
    
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, HLCTimestamp(10)) :: Nil, txUUID2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, lu(op0, db)), awaitDuration)
    
    errs2.toSet should be (Set(KeyValueRequirementError(op0, key)))
  }
  
  test("No lock conflicts for differing keys") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
        
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(k(2).bytes, List[Byte](3,4).toArray, HLCTimestamp(10))) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val errs = Await.result(ds.lockTransaction(txd, lu(op0, db)), awaitDuration)
    
    errs should be (Nil)
    
    val key2 = k(3)
    val reqs2 = List(KeyValueUpdate.KVRequirement(key2, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
    
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs2, HLCTimestamp(10)) :: Nil, txUUID2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, lu(op0, db)), awaitDuration)
    
    errs2.toSet should be (Set())
  }
  
  test("Key existece required, failure") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.Exists))
        
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(k(2).bytes, List[Byte](3,4).toArray, HLCTimestamp(10))) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val errs = Await.result(ds.lockTransaction(txd, lu(op0, db)), awaitDuration)
    
    errs.toSet should be (Set(KeyValueRequirementError(op0, key)))
  }
  
  test("Key existence required, success") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
        
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(key.bytes, List[Byte](3,4).toArray, HLCTimestamp(10))) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    Await.result(ds.commitTransactionUpdates(txd, lu(op0, db)), awaitDuration)
    
    val reqs2 = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.Exists))
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs2, HLCTimestamp(10)) :: Nil, txUUID2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, lu(op0, db)), awaitDuration)
    
    errs2.toSet should be (Set())
  }
  
  test("Key timestamp equals, failure") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    val keyts = HLCTimestamp(10)
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
        
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(key.bytes, List[Byte](3,4).toArray, keyts)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    Await.result(ds.commitTransactionUpdates(txd, lu(op0, db)), awaitDuration)
    
    val reqs2 = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.Equals))
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs2, HLCTimestamp(10)) :: Nil, txUUID2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, lu(op0, db)), awaitDuration)
    
    errs2.toSet should be (Set(KeyValueRequirementError(op0, key)))
  }
  
  test("Key timestamp equals, success") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    val keyts = HLCTimestamp(10)
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
        
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(key.bytes, List[Byte](3,4).toArray, keyts)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    Await.result(ds.commitTransactionUpdates(txd, lu(op0, db)), awaitDuration)
    
    val reqs2 = List(KeyValueUpdate.KVRequirement(key, keyts, KeyValueUpdate.TimestampRequirement.Equals))
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs2, HLCTimestamp(10)) :: Nil, txUUID2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, lu(op0, db)), awaitDuration)
    
    errs2.toSet should be (Set())
  }
  
  test("Key timestamp less than, failure") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    val keyts = HLCTimestamp(10)
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
        
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(key.bytes, List[Byte](3,4).toArray, keyts)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    Await.result(ds.commitTransactionUpdates(txd, lu(op0, db)), awaitDuration)
    
    val reqs2 = List(KeyValueUpdate.KVRequirement(key, keyts, KeyValueUpdate.TimestampRequirement.LessThan))
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs2, HLCTimestamp(10)) :: Nil, txUUID2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, lu(op0, db)), awaitDuration)
    
    errs2.toSet should be (Set(KeyValueRequirementError(op0, key)))
  }
  
  test("Key timestamp less than, success") {
    val (ds, op0, op1) = initObjects()
    val key = k(2)
    val keyts = HLCTimestamp(10)
    val reqs = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
        
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs, initialTimestamp) :: Nil)
    
    val ops = List(new Insert(key.bytes, List[Byte](3,4).toArray, keyts)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    Await.result(ds.commitTransactionUpdates(txd, lu(op0, db)), awaitDuration)
    
    val reqs2 = List(KeyValueUpdate.KVRequirement(key, HLCTimestamp(5), KeyValueUpdate.TimestampRequirement.LessThan))
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs2, HLCTimestamp(10)) :: Nil, txUUID2)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, lu(op0, db)), awaitDuration)
    
    errs2.toSet should be (Set())
  }
  
  test("Concurrent Transactions In Order Commit") {
    val (ds, op0, op1) = initObjects()
    
    val key1 = k(2)
    val key2 = k(3)
    val key3 = k(4)
    val value = List[Byte](3,4).toArray
    val value2 = List[Byte](5,6).toArray
    val valuets = HLCTimestamp(10)
    val valuets2 = HLCTimestamp(15)
    
    val reqs1 = List(KeyValueUpdate.KVRequirement(key1, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs1, initialTimestamp) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    val ops = List(new Insert(key1.bytes, value, valuets)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val updates = lu(op0, db)
    
    //-- 
    val reqs2 = List(KeyValueUpdate.KVRequirement(key2, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs2, txdts) :: Nil, txUUID2)
                    
    val txdts2 = HLCTimestamp(txd2.startTimestamp)
    
    val ops2 = List(new Insert(key2.bytes, value2, valuets2))
    val db2 = KeyValueObjectCodec.encodeUpdate(ida, ops2)(0)
    
    val updates2 = lu(op0, db2)
    //--
    
    val errs = Await.result(ds.lockTransaction(txd, updates), awaitDuration)
    
    errs should be (Nil)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, updates2), awaitDuration)
    
    errs2 should be (Nil)
    
    Await.result(ds.commitTransactionUpdates(txd, updates), awaitDuration)
    
    var s0 = get(ds, op0)
    
    s0.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s0.kv.idaEncodedContents.size should be (1)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key1).value, value) should be (true)
    s0.kv.idaEncodedContents(key1).timestamp should be (valuets)
    s0.locks should be (Set())
    
    Await.result(ds.commitTransactionUpdates(txd2, updates2), awaitDuration)
    
    s0 = get(ds, op0)
    
    s0.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s0.kv.idaEncodedContents.size should be (2)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key1).value, value) should be (true)
    s0.kv.idaEncodedContents(key1).timestamp should be (valuets)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key2).value, value2) should be (true)
    s0.kv.idaEncodedContents(key2).timestamp should be (valuets2)
    s0.locks should be (Set())
  }
  
  test("Concurrent Transactions Out of Order Commit") {
    val (ds, op0, op1) = initObjects()
    
    val key1 = k(2)
    val key2 = k(3)
    val key3 = k(4)
    val value = List[Byte](3,4).toArray
    val value2 = List[Byte](5,6).toArray
    val valuets = HLCTimestamp(10)
    val valuets2 = HLCTimestamp(15)
    
    val reqs1 = List(KeyValueUpdate.KVRequirement(key1, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
    val txd = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs1, initialTimestamp) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    val ops = List(new Insert(key1.bytes, value, valuets)) 
    val db = KeyValueObjectCodec.encodeUpdate(ida, ops)(0)
    
    val updates = lu(op0, db)
    
    //-- 
    val reqs2 = List(KeyValueUpdate.KVRequirement(key2, HLCTimestamp(0), KeyValueUpdate.TimestampRequirement.DoesNotExist))
    val txd2 = mktxd(KeyValueUpdate(op0, KeyValueUpdate.UpdateType.Append, None, reqs2, txdts) :: Nil, txUUID2)
                    
    val txdts2 = HLCTimestamp(txd2.startTimestamp)
    
    val ops2 = List(new Insert(key2.bytes, value2, valuets2))
    val db2 = KeyValueObjectCodec.encodeUpdate(ida, ops2)(0)
    
    val updates2 = lu(op0, db2)
    //--
    
    val errs = Await.result(ds.lockTransaction(txd, updates), awaitDuration)
    
    errs should be (Nil)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, updates2), awaitDuration)
    
    errs2 should be (Nil)
    
    Await.result(ds.commitTransactionUpdates(txd2, updates2), awaitDuration)
    
    var s0 = get(ds, op0)
    
    s0.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s0.kv.idaEncodedContents.size should be (1)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key2).value, value2) should be (true)
    s0.kv.idaEncodedContents(key2).timestamp should be (valuets2)
    s0.locks should be (Set())
    
    Await.result(ds.commitTransactionUpdates(txd, updates), awaitDuration)
    
    s0 = get(ds, op0)
    
    s0.md should be (ObjectMetadata(irev, ObjectRefcount(0,1), initialTimestamp))
    s0.kv.idaEncodedContents.size should be (2)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key1).value, value) should be (true)
    s0.kv.idaEncodedContents(key1).timestamp should be (valuets)
    java.util.Arrays.equals(s0.kv.idaEncodedContents(key2).value, value2) should be (true)
    s0.kv.idaEncodedContents(key2).timestamp should be (valuets2)
    s0.locks should be (Set())
  }
}