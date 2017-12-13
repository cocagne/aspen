package com.ibm.aspen.core.data_store

import scala.concurrent._
import scala.concurrent.duration._
import org.scalatest._
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.transaction.DataUpdate
import com.ibm.aspen.core.transaction.DataUpdateOperation
import com.ibm.aspen.core.transaction.RefcountUpdate
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.util.Success
import scala.util.Failure
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.HLCTimestamp

object DataStoreSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
  val uuid0 = new UUID(0,0)
  val uuid1 = new UUID(0,1)
  val uuid2 = new UUID(0,4)
  val poolUUID = new UUID(0,2)
  val txUUID = new UUID(0,3)
  val allocUUID = new UUID(0,4)
  
  val allocObj = ObjectPointer(new UUID(0,4), poolUUID, None, Replication(3,2), new Array[StorePointer](0))
  val allocRev = ObjectRevision(0,1)
  val oneRef = ObjectRefcount(0,1)
  
  val timestamp = HLCTimestamp.now
  
  def mkObjPtr(objUUID:UUID, sp:StorePointer) = ObjectPointer(objUUID, poolUUID, None, Replication(3,2), (sp::Nil).toArray)
  
  def mktxd(du: List[DataUpdate], ru: List[RefcountUpdate], txdUUID:UUID=txUUID) = {
    TransactionDescription(txdUUID, 100, allocObj, 0, du ++ ru, Nil)
  }
  
  def mklu(objectPointer: ObjectPointer): Option[List[LocalUpdate]] = Some(List(LocalUpdate(objectPointer.uuid, DataBuffer(ByteBuffer.allocate(0)))))
  
  val storeId = DataStoreID(poolUUID, 1)
  
  val icontent0 = DataBuffer(List[Byte](1,2,3).toArray)
  val icontent1 = DataBuffer(List[Byte](4,5,6).toArray)
  val irev = ObjectRevision(0,3)
}

abstract class DataStoreSuite extends AsyncFunSuite with Matchers {
  import DataStoreSuite._
  
  def newStore: DataStore
  
  // Helper method that creates a store and adds two objects. Returns (DataStore, Obj0StorePointer, Obj1StorePointer)
  def initObjects(): (DataStore, StorePointer, StorePointer) = {
    val ds = newStore
    
    val expected = (CurrentObjectState(uuid0, irev, oneRef, timestamp, None), icontent0)
    
    implicit val executionContext = ExecutionContext.Implicits.global
    
    val lno0 = List(Allocate.NewObject(uuid0, None, oneRef, icontent0))
    val lno1 = List(Allocate.NewObject(uuid1, None, oneRef, icontent1))
    
    val f = ds.allocate(lno0, timestamp, allocUUID, allocObj, allocRev) flatMap { either => either match {
      case Right(ars0) => ds.allocate(lno1, timestamp, allocUUID, allocObj, allocRev).flatMap(er => er match {
        case Right(ars1) => Future.successful((ds, ars0.newObjects.head.storePointer, ars1.newObjects.head.storePointer))
        case Left(err) => fail("Returned failure instead of object content")
      })
      case Left(err) => fail("Returned failure instead of store pointer")
    }}
    
    Await.result(f, awaitDuration)
  }
  
  // Helper method that reads an object and validates it's state
  def checkState(ds: DataStore, op: ObjectPointer, cs: CurrentObjectState, obuf: Option[DataBuffer] = None, ignoreTimestamp: Boolean = false) = {
    val r = Await.result(ds.getObject(op), awaitDuration)
    r match {      
      case Left(_) => fail
      case Right(t) => 
        obuf.foreach(buf => t._2 should be (buf))
        if (ignoreTimestamp)
          t._1.copy(timestamp=HLCTimestamp(0)) should be (cs.copy(timestamp=HLCTimestamp(0)))
        else
          t._1 should be (cs)
    }
  }
  
  test("Discard Locked Transaction") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, newRef) :: Nil)
                  
    val err1 = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    if (!err1.isEmpty)
       fail(s"Shouldn't have encountered errors: $err1")
    
    ds.discardTransaction(txd)
    
    // Ensure new Tx can lock against unmodified objects
    val tx2UUID = new UUID(99,99)
    
    val txd2 = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, newRef) :: Nil, tx2UUID)
                    
    val err2 = Await.result(ds.lockTransaction(txd2, mklu(op0)), awaitDuration)
    
    if (!err2.isEmpty)
       fail(s"Shouldn't have encountered errors: $err2")
    else
      succeed
  }
  
  test("Read Locked Object") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, newRef) :: Nil)
              
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    errs should be (Nil)
    
    checkState(ds, op0, CurrentObjectState(uuid0, irev, oneRef, txdts, Some(txd)), ignoreTimestamp=true)
    checkState(ds, op1, CurrentObjectState(uuid1, irev, oneRef, txdts, Some(txd)), ignoreTimestamp=true)
    
  }
  
  test("Commit Locked Transaction") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, newRef) :: Nil)
                    
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs.isEmpty should be (true)
    
    val newContent = DataBuffer(List[Byte](7,8,9,10).toArray)
    
    val lu = Some(List(LocalUpdate(op0.uuid, newContent)))
    
    Await.result(ds.commitTransactionUpdates(txd, lu), awaitDuration)
    
    val newRev = ObjectRevision(1,4)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    checkState(ds, op0, CurrentObjectState(uuid0, newRev, oneRef, txdts, None), Some(newContent))
    checkState(ds, op1, CurrentObjectState(uuid1, ObjectRevision(0,3), newRef, txdts, None), Some(icontent1))
    
    // Ensure new Tx can lock against updated attributes
    val tx2UUID = new UUID(99,99)
    
    
    val txd2 = mktxd(DataUpdate(op0, newRev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, newRef, newRef) :: Nil, tx2UUID)
                    
    val errs2 = Await.result(ds.lockTransaction(txd2, mklu(op0)), awaitDuration)
    
    errs2.isEmpty should be (true)
  }
  
  test("Lock With Collision and Error") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs.isEmpty should be (true)
    
    val tx2UUID = new UUID(99,99)
    
    val op3 = mkObjPtr(uuid2, StorePointer(storeId.poolIndex, List[Byte](1,2,3,4).toArray))
    
    val txd2 = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op3, oneRef, oneRef) :: Nil, tx2UUID)
        
    val errs2 = Await.result(ds.lockTransaction(txd2, mklu(op0)), awaitDuration)
    
    errs2 should be (List(TransactionCollision(op0, txd), TransactionReadError(op3, InvalidLocalPointer())))
  }
  
  test("Lock With Revision and Refcount Errors") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    
    val badRev = ObjectRevision(3,4)
    val badRef = ObjectRefcount(5,6)
    
    val txd = mktxd(DataUpdate(op0, badRev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, badRef, oneRef) :: Nil)
         
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs should be (List(RevisionMismatch(op0, badRev, irev), RefcountMismatch(op1, badRef, oneRef)))
    
  }
  
  test("Lock With Collisions") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs.isEmpty should be (true)
    
    val tx2UUID = new UUID(99,99)
    
    val txd2 = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, oneRef) :: Nil, tx2UUID)
        
    val errs2 = Await.result(ds.lockTransaction(txd2, mklu(op0)), awaitDuration)
    
    errs2 should be (List(TransactionCollision(op0, txd), TransactionCollision(op1, txd)))
  }
  
  test("Lock No Collisions") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs should be (Nil)
  }
  
  test("Get Object State") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
                    
    checkState(ds, op0, CurrentObjectState(uuid0, irev, oneRef, txdts, None), ignoreTimestamp=true)
    checkState(ds, op1, CurrentObjectState(uuid1, irev, oneRef, txdts, None), ignoreTimestamp=true)
  }
  
  test("Get Invalid Object State") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op2 = mkObjPtr(uuid2, StorePointer(storeId.poolIndex, List[Byte](1,2,3,4).toArray))
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op2, oneRef, oneRef) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
                    
    checkState(ds, op0, CurrentObjectState(uuid0, irev, oneRef, txdts, None), ignoreTimestamp=true)
    
    val r = Await.result(ds.getObject(op2), awaitDuration)
    r should matchPattern { case Left(e: InvalidLocalPointer) => }
  }
  
  test("Get Object State With Object Mistmatch") {
    val (ds, sp0, sp1) = initObjects()
    
    val badUUID = new UUID(99,99)
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(badUUID, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
                    
    checkState(ds, op0, CurrentObjectState(uuid0, irev, oneRef, txdts, None), ignoreTimestamp=true)
    
    val r = Await.result(ds.getObject(op1), awaitDuration)
    r should matchPattern { 
      case Left(e: ObjectMismatch) =>
      case Left(e: InvalidLocalPointer) => 
    }
  }
  
  test("Allocate New Object") {
    val ds = newStore
    
    val icontent = DataBuffer(List[Byte](1,2,3).toArray)
    val lno = List(Allocate.NewObject(uuid0, None, oneRef, icontent))
    val futureResponse = ds.allocate(lno, timestamp, txUUID, allocObj, allocRev)
            
    futureResponse map { either => either match {
      case Right(ars) => ars.newObjects.head.storePointer.poolIndex should be (ds.storeId.poolIndex)
      case Left(err) => fail("Returned failure instead of store pointer")
    }}
	}
  
  test("Allocate and Read New Object") {
    val ds = newStore
    
    val icontent = DataBuffer(List[Byte](1,2,3).toArray)
    val lno = List(Allocate.NewObject(uuid0, None, oneRef, icontent))
    val futureResponse = ds.allocate(lno, timestamp, txUUID, allocObj, allocRev)
            
    val expected = (CurrentObjectState(uuid0, ObjectRevision(0,3), oneRef, timestamp, None), icontent)
    
    futureResponse flatMap { either => either match {
      case Right(ars) => ds.getObject(mkObjPtr(uuid0, ars.newObjects.head.storePointer)).flatMap(er => er match {
        case Right(data) => data should be (expected)
        case Left(err) => fail(s"Returned failure instead of object content: $err")
      })
      case Left(err) => fail("Returned failure instead of store pointer")
    }}
	}
  
  test("Read Invalid Object") {
    val ds = newStore
    val sp = StorePointer(storeId.poolIndex, new Array[Byte](0))
    val futureResponse = ds.getObject(mkObjPtr(txUUID, sp))
    
    futureResponse map { either => either match {
      case Right(_) => fail("Should have failed read")
      case Left(err) => err should matchPattern{ case _:InvalidLocalPointer => }
    }}
	}
  
}