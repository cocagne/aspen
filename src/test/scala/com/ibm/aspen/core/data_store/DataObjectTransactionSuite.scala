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
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.allocation.DataAllocationOptions
import com.ibm.aspen.core.transaction.VersionBump
import com.ibm.aspen.core.transaction.TransactionRequirement
import com.ibm.aspen.core.transaction.RevisionLock

object DataObjectTransactionSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
  val uuid0 = new UUID(0,0)
  val uuid1 = new UUID(0,1)
  val uuid2 = new UUID(0,4)
  val poolUUID = new UUID(0,2)
  val txUUID = new UUID(0,3)
  val allocUUID = new UUID(0,4)
  
  val allocObj = DataObjectPointer(new UUID(0,4), poolUUID, None, Replication(3,2), new Array[StorePointer](0))
  val allocRev = ObjectRevision.Null
  val oneRef = ObjectRefcount(0,1)
  
  val timestamp = HLCTimestamp.now
  
  def mkObjPtr(objUUID:UUID, sp:StorePointer) = DataObjectPointer(objUUID, poolUUID, None, Replication(3,2), (sp::Nil).toArray)
  
  def mktxd(reqs: List[TransactionRequirement], txdUUID:UUID=txUUID) = {
    TransactionDescription(txdUUID, 100, allocObj, 0, reqs, Nil)
  }
  
  def mklu(objectPointer: ObjectPointer): Option[List[LocalUpdate]] = Some(List(LocalUpdate(objectPointer.uuid, DataBuffer(ByteBuffer.allocate(0)))))
  
  val storeId = DataStoreID(poolUUID, 1)
  
  val icontent0 = DataBuffer(List[Byte](1,2,3).toArray)
  val icontent1 = DataBuffer(List[Byte](4,5,6).toArray)
  val irev = ObjectRevision(allocUUID)
}

class DataObjectTransactionSuite extends AsyncFunSuite with Matchers {
  import DataObjectTransactionSuite._
  
  def newStore: DataStore = new DataStoreFrontend(DataObjectTransactionSuite.storeId, 
      new MemoryOnlyDataStoreBackend()(ExecutionContext.Implicits.global), Nil, Nil)
  
  // Helper method that creates a store and adds two objects. Returns (DataStore, Obj0StorePointer, Obj1StorePointer)
  def initObjects(): (DataStore, StorePointer, StorePointer) = {
    val ds = newStore

    implicit val executionContext = ExecutionContext.Implicits.global
    
    val f = ds.allocate(uuid0, new DataAllocationOptions, None, oneRef, icontent0, timestamp, allocUUID, allocObj, allocRev) flatMap { either => either match {
      case Right(ars0) => ds.allocate(uuid1, new DataAllocationOptions, None, oneRef, icontent1, timestamp, allocUUID, allocObj, allocRev).flatMap(er => er match {
        case Right(ars1) => Future.successful((ds, ars0.storePointer, ars1.storePointer))
        case Left(err) => fail("Returned failure instead of object content")
      })
      case Left(err) => fail("Returned failure instead of store pointer")
    }}
    
    Await.result(f, awaitDuration)
  }
  
  // Helper method that reads an object and validates it's state
  def checkState(ds: DataStore, op: ObjectPointer, md: ObjectMetadata, locks: List[Lock], obuf: Option[DataBuffer] = None, ignoreTimestamp: Boolean = false) = {
    val r = Await.result(ds.getObject(op), awaitDuration)
    r match {      
      case Left(_) => fail
      case Right((rmeta, rdata, rlocks)) =>
        obuf.foreach(buf => rdata should be (buf))
        
        if (ignoreTimestamp)
          rmeta.copy(timestamp=HLCTimestamp(0)) should be (md.copy(timestamp=HLCTimestamp(0)))
        else
          rmeta should be (md)
        
        rlocks.toSet should be (locks.toSet)
    }
  }
  
  test("Get Object State") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
                    
    checkState(ds, op0, ObjectMetadata(irev, oneRef, txdts), Nil, ignoreTimestamp=true)
    checkState(ds, op1, ObjectMetadata(irev, oneRef, txdts), Nil, ignoreTimestamp=true)
  }
  
  test("Get Invalid Object State") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op2 = mkObjPtr(uuid2, StorePointer(storeId.poolIndex, List[Byte](1,2,3,4).toArray))
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op2, oneRef, oneRef) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
                    
    checkState(ds, op0, ObjectMetadata(irev, oneRef, txdts), Nil, ignoreTimestamp=true)
    
    val r = Await.result(ds.getObject(op2), awaitDuration)
    r should matchPattern { case Left(e: InvalidLocalPointer) => }
  }
  
  test("Get Object State With Object Mistmatch") {
    val (ds, sp0, sp1) = initObjects()
    
    val badUUID = new UUID(99,99)
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(badUUID, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val txdts = HLCTimestamp(txd.startTimestamp)
                    
    checkState(ds, op0, ObjectMetadata(irev, oneRef, txdts), Nil, ignoreTimestamp=true)
    
    val r = Await.result(ds.getObject(op1), awaitDuration)
    r should matchPattern { 
      case Left(e: ObjectMismatch) =>
      case Left(e: InvalidLocalPointer) => 
    }
  }
  
  test("Allocate New Object") {
    val ds = newStore
    
    val icontent = DataBuffer(List[Byte](1,2,3).toArray)
    
    val futureResponse = ds.allocate(uuid0, new DataAllocationOptions, None, oneRef, icontent, timestamp, txUUID, allocObj, allocRev)
            
    futureResponse map { either => either match {
      case Right(ars) => ars.storePointer.poolIndex should be (ds.storeId.poolIndex)
      case Left(err) => fail("Returned failure instead of store pointer")
    }}
	}
  
  test("Allocate and Read New Object") {
    val ds = newStore
    
    val icontent = DataBuffer(List[Byte](1,2,3).toArray)
    
    val futureResponse = ds.allocate(uuid0, new DataAllocationOptions, None, oneRef, icontent, timestamp, txUUID, allocObj, allocRev)
            
    val expected = (ObjectMetadata(ObjectRevision(txUUID), oneRef, timestamp), icontent, Nil)
    
    futureResponse flatMap { either => either match {
      case Right(ars) => ds.getObject(mkObjPtr(uuid0, ars.storePointer)).flatMap(er => er match {
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
  
  test("Lock No Collisions") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs should be (Nil)
  }
  
  test("Discard Locked Transaction") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, newRef) :: Nil)
                  
    val err1 = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    if (!err1.isEmpty)
       fail(s"Shouldn't have encountered errors: $err1")
    
    ds.discardTransaction(txd)
    
    // Ensure new Tx can lock against unmodified objects
    val tx2UUID = new UUID(99,99)
    
    val txd2 = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, newRef) :: Nil, tx2UUID)
                    
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
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, newRef) :: Nil)
              
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    errs should be (Nil)
    
    checkState(ds, op0, ObjectMetadata(irev, oneRef, txdts), List(RevisionWriteLock(txd)), ignoreTimestamp=true)
    checkState(ds, op1, ObjectMetadata(irev, oneRef, txdts), List(RefcountWriteLock(txd)), ignoreTimestamp=true)
    
  }
  
  test("Commit Locked Transaction") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, newRef) :: Nil)
                    
    val newContent = DataBuffer(List[Byte](7,8,9,10).toArray)
    
    val lu = Some(List(LocalUpdate(op0.uuid, newContent)))
    
    val errs = Await.result(ds.lockTransaction(txd, lu), awaitDuration)
    //println(s"Errors is $errs")
    errs.isEmpty should be (true)
    
    Await.result(ds.commitTransactionUpdates(txd, lu), awaitDuration)
    
    val newRev = ObjectRevision(txd.transactionUUID)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    checkState(ds, op0, ObjectMetadata(newRev, oneRef, txdts), Nil, Some(newContent))
    checkState(ds, op1, ObjectMetadata(irev, newRef, txdts), Nil, Some(icontent1))
    
    // Ensure new Tx can lock against updated attributes
    val tx2UUID = new UUID(99,99)
    
    
    val txd2 = mktxd(DataUpdate(op0, newRev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, newRef, newRef) :: Nil, tx2UUID)
                    
    val errs2 = Await.result(ds.lockTransaction(txd2, mklu(op0)), awaitDuration)
    
    errs2.isEmpty should be (true)
  }
  
  test("Lock With Collision and Error") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs.isEmpty should be (true)
    
    val tx2UUID = new UUID(99,99)
    
    val op3 = mkObjPtr(uuid2, StorePointer(storeId.poolIndex, List[Byte](1,2,3,4).toArray))
    
    val txd2 = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op3, oneRef, oneRef) :: Nil, tx2UUID)
        
    val errs2 = Await.result(ds.lockTransaction(txd2, mklu(op0)), awaitDuration)
    
    errs2.toSet should be (Set(TransactionCollision(op0, txd), TransactionReadError(op3, InvalidLocalPointer())))
  }
  
  test("Lock With Revision and Refcount Errors") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    
    val badRev = ObjectRevision(new UUID(0,3))
    val badRef = ObjectRefcount(5,6)
    
    val txd = mktxd(DataUpdate(op0, badRev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, badRef, oneRef) :: Nil)
         
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs.toSet should be (Set(RevisionMismatch(op0, badRev, irev), RefcountMismatch(op1, badRef, oneRef)))
  }
  
  test("Lock With Version Bump Revision Error") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)

    val badRev = ObjectRevision(new UUID(0,3))
    
    val txd = mktxd(VersionBump(op0, badRev) :: Nil)
         
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs.toSet should be (Set(RevisionMismatch(op0, badRev, irev)))
  }
  
  test("Lock With RevisionLock Revision Error") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)

    val badRev = ObjectRevision(new UUID(0,3))
    
    val txd = mktxd(RevisionLock(op0, badRev) :: Nil)
         
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs.toSet should be (Set(RevisionMismatch(op0, badRev, irev)))
  }
  
  test("Multiple RevisionLocks do not cause error") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    
    val txd = mktxd(RevisionLock(op0, irev) :: Nil)
         
    val errs = Await.result(ds.lockTransaction(txd, None), awaitDuration)
    
    errs.toSet should be (Set())
    
    val txd2 = mktxd(RevisionLock(op0, irev) :: Nil)
    
    val errs2 = Await.result(ds.lockTransaction(txd2, None), awaitDuration)
    
    errs2.toSet should be (Set())
  }
  
  test("Lock With Collisions") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs.isEmpty should be (true)
    
    val tx2UUID = new UUID(99,99)
    
    val txd2 = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, oneRef) :: Nil, tx2UUID)
        
    val errs2 = Await.result(ds.lockTransaction(txd2, mklu(op0)), awaitDuration)
    
    errs2.toSet should be (Set(TransactionCollision(op0, txd), TransactionCollision(op1, txd)))
  }
  
  test("RevisionLock Causes Update Collisions") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(RevisionLock(op0, irev) :: RefcountUpdate(op1, oneRef, oneRef) :: Nil)
                    
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    errs.isEmpty should be (true)
    
    val tx2UUID = new UUID(99,99)
    
    val txd2 = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, oneRef) :: Nil, tx2UUID)
        
    val errs2 = Await.result(ds.lockTransaction(txd2, mklu(op0)), awaitDuration)
    
    errs2.toSet should be (Set(TransactionCollision(op0, txd), TransactionCollision(op1, txd)))
  }
  
  test("Commit Unlocked Transaction") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, newRef) :: Nil)
                    
    val newContent = DataBuffer(List[Byte](7,8,9,10).toArray)
    
    val lu = Some(List(LocalUpdate(op0.uuid, newContent)))

    Await.result(ds.commitTransactionUpdates(txd, lu), awaitDuration)
    
    val newRev = ObjectRevision(txd.transactionUUID)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    checkState(ds, op0, ObjectMetadata(newRev, oneRef, txdts), Nil, Some(newContent))
    checkState(ds, op1, ObjectMetadata(irev, newRef, txdts), Nil, Some(icontent1))
    
    // Ensure new Tx can lock against updated attributes
    val tx2UUID = new UUID(99,99)
    
    
    val txd2 = mktxd(DataUpdate(op0, newRev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, newRef, newRef) :: Nil, tx2UUID)
                    
    val errs2 = Await.result(ds.lockTransaction(txd2, mklu(op0)), awaitDuration)
    
    errs2.isEmpty should be (true)
  }
  
  test("Commit Unlocked Transaction with Invalid Object") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val bad = mkObjPtr(new UUID(9,9), StorePointer(storeId.poolIndex, new Array[Byte](0)))
    
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: 
                    DataUpdate(bad, irev, DataUpdateOperation.Overwrite) ::  
                    RefcountUpdate(op1, oneRef, newRef) :: Nil)
                    
    val newContent = DataBuffer(List[Byte](7,8,9,10).toArray)
    
    val lu = Some(List(LocalUpdate(op0.uuid, newContent), LocalUpdate(bad.uuid, newContent)))

    Await.result(ds.commitTransactionUpdates(txd, lu), awaitDuration)
    
    val newRev = ObjectRevision(txd.transactionUUID)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    checkState(ds, op0, ObjectMetadata(newRev, oneRef, txdts), Nil, Some(newContent))
    checkState(ds, op1, ObjectMetadata(irev, newRef, txdts), Nil, Some(icontent1))
  }
  
  test("Commit Unlocked Transaction with Different Locked Transaction") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: Nil)
                     
    val tx2UUID = new UUID(99,99)
    
    val txd2 = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, newRef) :: Nil, tx2UUID)
                    
    val newContent = DataBuffer(List[Byte](7,8,9,10).toArray)
    
    val lu = Some(List(LocalUpdate(op0.uuid, newContent)))
    
    val errs = Await.result(ds.lockTransaction(txd, lu), awaitDuration)
    
    errs.isEmpty should be (true)
    
    checkState(ds, op0, ObjectMetadata(irev, oneRef, timestamp), List(RevisionWriteLock(txd)), Some(icontent0))
    checkState(ds, op1, ObjectMetadata(irev, oneRef, timestamp), Nil, Some(icontent1))
    
    // Commit different Tx. Data revision should stay the same on op0 but refcount should change for op1
    Await.result(ds.commitTransactionUpdates(txd2, lu), awaitDuration)
    
    val txdts = HLCTimestamp(txd2.startTimestamp)
    
    checkState(ds, op0, ObjectMetadata(irev, oneRef, timestamp), List(RevisionWriteLock(txd)), Some(icontent0))
    checkState(ds, op1, ObjectMetadata(irev, newRef, txdts), Nil, Some(icontent1))
  }
  
  test("Commit Unlocked Transaction with Missing Data") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: RefcountUpdate(op1, oneRef, newRef) :: Nil)
                    
    val newContent = DataBuffer(List[Byte](7,8,9,10).toArray)

    Await.result(ds.commitTransactionUpdates(txd, None), awaitDuration)
    
    val newRev = ObjectRevision(txd.transactionUUID)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    checkState(ds, op0, ObjectMetadata(irev, oneRef, timestamp), Nil, Some(icontent0))
    checkState(ds, op1, ObjectMetadata(irev, newRef, txdts), Nil, Some(icontent1))
  }
  
  test("Commit Append Transaction") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Append) :: RefcountUpdate(op1, oneRef, newRef) :: Nil)
                    
    val newContent = DataBuffer(List[Byte](7,8,9,10).toArray)
    
    val bb = ByteBuffer.allocate(icontent0.size + newContent.size)
    bb.put(icontent0)
    bb.put(newContent)
    bb.position(0)
    
    val appended = DataBuffer(bb)
    
    val lu = Some(List(LocalUpdate(op0.uuid, newContent)))

    Await.result(ds.commitTransactionUpdates(txd, lu), awaitDuration)
    
    val newRev = ObjectRevision(txd.transactionUUID)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    checkState(ds, op0, ObjectMetadata(newRev, oneRef, txdts), Nil, Some(appended))
    checkState(ds, op1, ObjectMetadata(irev, newRef, txdts), Nil, Some(icontent1))
  }
  
  test("Commit VersionBump Transaction") {
    val (ds, sp0, sp1) = initObjects()
    
    val newRef = ObjectRefcount(0,2)
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(VersionBump(op0, irev) :: RefcountUpdate(op1, oneRef, newRef) :: Nil)

    Await.result(ds.commitTransactionUpdates(txd, None), awaitDuration)
    
    val newRev = ObjectRevision(txd.transactionUUID)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    checkState(ds, op0, ObjectMetadata(newRev, oneRef, txdts), Nil, Some(icontent0))
    checkState(ds, op1, ObjectMetadata(irev, newRef, txdts), Nil, Some(icontent1))
  }
  
  test("Fail lock on revision collision") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(DataUpdate(op0, irev, DataUpdateOperation.Overwrite) :: VersionBump(op1, irev) :: Nil)
              
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    errs should be (Nil)
    
    checkState(ds, op0, ObjectMetadata(irev, oneRef, timestamp), List(RevisionWriteLock(txd)))
    checkState(ds, op1, ObjectMetadata(irev, oneRef, timestamp), List(RevisionWriteLock(txd)))
    
    val txd2 = mktxd(VersionBump(op0, irev) :: DataUpdate(op1, irev, DataUpdateOperation.Overwrite) :: Nil, new UUID(99,99))
    
    val errs2 = Await.result(ds.lockTransaction(txd2, None), awaitDuration)
    
    errs2.toSet should be (Set(new MissingUpdateContent(op1), new TransactionCollision(op0, txd), new TransactionCollision(op1, txd)))
  }
  
  test("Fail lock on RevisionLock collision") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val txd = mktxd(RevisionLock(op0, irev) :: VersionBump(op1, irev) :: Nil)
              
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    errs should be (Nil)
    
    checkState(ds, op0, ObjectMetadata(irev, oneRef, timestamp), List(RevisionReadLock(txd)))
    checkState(ds, op1, ObjectMetadata(irev, oneRef, timestamp), List(RevisionWriteLock(txd)))
    
    val txd2 = mktxd(VersionBump(op0, irev) :: DataUpdate(op1, irev, DataUpdateOperation.Overwrite) :: Nil, new UUID(99,99))
    
    val errs2 = Await.result(ds.lockTransaction(txd2, None), awaitDuration)
    
    errs2.toSet should be (Set(new MissingUpdateContent(op1), new TransactionCollision(op0, txd), new TransactionCollision(op1, txd)))
  }
  
  test("Fail lock on refcount collision") {
    val (ds, sp0, sp1) = initObjects()
    
    val op0 = mkObjPtr(uuid0, sp0)
    val op1 = mkObjPtr(uuid1, sp1)
    val newRef = ObjectRefcount(0,2)
    
    val txd = mktxd(RefcountUpdate(op1, oneRef, newRef) :: Nil)
              
    val errs = Await.result(ds.lockTransaction(txd, mklu(op0)), awaitDuration)
    
    val txdts = HLCTimestamp(txd.startTimestamp)
    
    errs should be (Nil)
    
    checkState(ds, op0, ObjectMetadata(irev, oneRef, timestamp), Nil)
    checkState(ds, op1, ObjectMetadata(irev, oneRef, timestamp), List(RefcountWriteLock(txd)))
    
    val txd2 = mktxd(RefcountUpdate(op1, oneRef, newRef) :: Nil, new UUID(99,99))
    
    val errs2 = Await.result(ds.lockTransaction(txd2, None), awaitDuration)
    
    errs2.toSet should be (Set(new TransactionCollision(op1, txd)))
  }
}