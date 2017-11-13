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

object DataStoreSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
  val uuid0 = new UUID(0,0)
  val uuid1 = new UUID(0,1)
  val uuid2 = new UUID(0,4)
  val poolUUID = new UUID(0,2)
  val txUUID = new UUID(0,3)
  
  val allocObj = ObjectPointer(new UUID(0,4), poolUUID, None, Replication(3,2), new Array[StorePointer](0))
  val allocRev = ObjectRevision(0,1)
  val oneRef = ObjectRefcount(0,1)
  
  def mkObjPtr(objUUID:UUID, sp:StorePointer) = ObjectPointer(objUUID, poolUUID, None, Replication(3,2), (sp::Nil).toArray)
  
  def mktxd(du: List[DataUpdate], ru: List[RefcountUpdate], txdUUID:UUID=txUUID) = TransactionDescription(txdUUID, 100, allocObj, 0, du, ru, Nil)
  
  def mklu(objectPointer: ObjectPointer): Option[List[LocalUpdate]] = Some(List(LocalUpdate(objectPointer.uuid, ByteBuffer.allocate(0))))
  
  val storeId = DataStoreID(poolUUID, 1)
  
  val icontent0 = ByteBuffer.wrap(List[Byte](1,2,3).toArray)
  val icontent1 = ByteBuffer.wrap(List[Byte](4,5,6).toArray)
  val irev = ObjectRevision(0,3)
}

abstract class DataStoreSuite extends AsyncFunSuite with Matchers {
  import DataStoreSuite._
  
  def newStore: DataStore
  
  // Helper method that creates a store and adds two objects. Returns (DataStore, Obj0StorePointer, Obj1StorePointer)
  def initObjects(): (DataStore, StorePointer, StorePointer) = {
    val ds = newStore
            
    val expected = (CurrentObjectState(uuid0, irev, oneRef, txUUID, None), icontent0)
    
    implicit val executionContext = ExecutionContext.Implicits.global
    
    val f = ds.allocateNewObject(uuid0, None, icontent0, oneRef, txUUID, allocObj, allocRev) flatMap { either => either match {
      case Right(sp0) => ds.allocateNewObject(uuid1, None, icontent1, oneRef, txUUID, allocObj, allocRev).flatMap(er => er match {
        case Right(sp1) => Future.successful((ds, sp0, sp1))
        case Left(err) => fail("Returned failure instead of object content")
      })
      case Left(err) => fail("Returned failure instead of store pointer")
    }}
    
    Await.result(f, awaitDuration)
  }
  
  // Helper method that reads an object and validates it's state
  def checkState(ds: DataStore, op: ObjectPointer, cs: CurrentObjectState, obuf: Option[ByteBuffer] = None) = {
    val r = Await.result(ds.getObject(op), awaitDuration)
    r match {      
      case Left(_) => fail
      case Right(t) => 
        obuf.foreach(buf => t._2 should be (buf))
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
    
    errs.isEmpty should be (true)
    
    checkState(ds, op0, CurrentObjectState(uuid0, irev, oneRef, txUUID, Some(txd)))
    checkState(ds, op1, CurrentObjectState(uuid1, irev, oneRef, txUUID, Some(txd)))
    
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
    
    val newContent = ByteBuffer.wrap(List[Byte](7,8,9,10).toArray)
    
    val lu = Some(List(LocalUpdate(op0.uuid, newContent)))
    
    Await.result(ds.commitTransactionUpdates(txd, lu), awaitDuration)
    
    val newRev = ObjectRevision(1,4)
    
    checkState(ds, op0, CurrentObjectState(uuid0, newRev, oneRef, txd.transactionUUID, None), Some(newContent))
    checkState(ds, op1, CurrentObjectState(uuid1, ObjectRevision(0,3), newRef, txd.transactionUUID, None), Some(icontent1))
    
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
    
    errs.isEmpty should be (true)
  }
  
  test("Get Object State") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    checkState(ds, op0, CurrentObjectState(uuid0, irev, oneRef, txUUID, None))
    checkState(ds, op1, CurrentObjectState(uuid1, irev, oneRef, txUUID, None))
  }
  
  test("Get Invalid Object State") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op2 = mkObjPtr(uuid2, StorePointer(storeId.poolIndex, List[Byte](1,2,3,4).toArray))
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil, 
                    RefcountUpdate(op2, oneRef, oneRef) :: Nil)
                    
    checkState(ds, op0, CurrentObjectState(uuid0, irev, oneRef, txUUID, None))
    
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
                    
    checkState(ds, op0, CurrentObjectState(uuid0, irev, oneRef, txUUID, None))
    
    val r = Await.result(ds.getObject(op1), awaitDuration)
    r should matchPattern { 
      case Left(e: ObjectMismatch) =>
      case Left(e: InvalidLocalPointer) => 
    }
  }
  
  test("Allocate New Object") {
    val ds = newStore
    
    val icontent = ByteBuffer.wrap(List[Byte](1,2,3).toArray)
    val futureResponse = ds.allocateNewObject(uuid0, None, icontent, oneRef, txUUID, allocObj, allocRev)
            
    futureResponse map { either => either match {
      case Right(sp) => sp.poolIndex should be (ds.storeId.poolIndex)
      case Left(err) => fail("Returned failure instead of store pointer")
    }}
	}
  
  test("Allocate and Read New Object") {
    val ds = newStore
    
    val icontent = ByteBuffer.wrap(List[Byte](1,2,3).toArray)
    val futureResponse = ds.allocateNewObject(uuid0, None, icontent, oneRef, txUUID, allocObj, allocRev)
            
    val expected = (CurrentObjectState(uuid0, ObjectRevision(0,3), oneRef, txUUID, None), icontent)
    
    futureResponse flatMap { either => either match {
      case Right(sp) => ds.getObject(mkObjPtr(uuid0, sp)).flatMap(er => er match {
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