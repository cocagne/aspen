package com.ibm.aspen.core.read

import java.util.UUID

import com.ibm.aspen.base.{AspenSystem, ObjectCache, OpportunisticRebuildManager}
import com.ibm.aspen.base.impl.TransactionStatusCache
import com.ibm.aspen.core.data_store.{DataStoreID, ObjectReadError}
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.network.{ClientID, ClientSideReadMessenger}
import com.ibm.aspen.core.objects._
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp, read}
import com.ibm.aspen.core.transaction.TransactionDescription
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

object BaseReadDriverSuite {
  val awaitDuration = Duration(100, MILLISECONDS)
  
  val objUUID = new UUID(0,1)
  val poolUUID = new UUID(0,2)
  val readUUID = new UUID(0,3)
  val cliUUID = new UUID(0,4)
  
  val ida = Replication(3,2)
  
  val ds0 = DataStoreID(poolUUID, 0)
  val ds1 = DataStoreID(poolUUID, 1)
  val ds2 = DataStoreID(poolUUID, 2)
  
  val sp0 = StorePointer(0, List[Byte](0).toArray)
  val sp1 = StorePointer(1, List[Byte](1).toArray)
  val sp2 = StorePointer(2, List[Byte](2).toArray)
  
  val ptr = DataObjectPointer(objUUID, poolUUID, None, ida, (sp0 :: sp1 :: sp2 :: Nil).toArray)
  val kvptr = KeyValueObjectPointer(objUUID, poolUUID, None, ida, (sp0 :: sp1 :: sp2 :: Nil).toArray)
  val rev = ObjectRevision.Null
  val ref = ObjectRefcount(1,1)
  
  val odata = DataBuffer(List[Byte](1,2,3,4).toArray)
  
  val noLocks = Some(Map[DataStoreID, List[TransactionDescription]]())
  
  val client = ClientID(cliUUID)
  
  class TMessenger extends ClientSideReadMessenger {
    var mlist = List[(DataStoreID,read.Message)]()

    def system: Option[AspenSystem] = None

    def send(message: read.Read): Unit = mlist = (message.toStore, message) :: mlist
    
    def send(message: read.OpportunisticRebuild): Unit = mlist = (message.toStore, message) :: mlist

    def send(message: read.TransactionCompletionQuery): Unit = ()
    
    def clear() = mlist = Nil
    
    val clientId = BaseReadDriverSuite.client
  }
}

class BaseReadDriverSuite  extends AsyncFunSuite with Matchers {
  import BaseReadDriverSuite._

  object noTxCache extends TransactionStatusCache {
    override def transactionAborted(txuuid: UUID): Unit = None

    override def transactionCommitted(txuuid: UUID): Unit = None

    override def transactionFinalized(txuuid: UUID): Unit = None
  }


  
  def mkReader(clientMessenger: ClientSideReadMessenger,
               objectPointer: ObjectPointer = ptr,
               readType: ReadType = FullObject(),
               retrieveLockedTransaction: Boolean = true,
               readUUID:UUID = readUUID,
               transactionStatusCache: TransactionStatusCache = noTxCache) = {
    new BaseReadDriver(ObjectCache.NoCache, OpportunisticRebuildManager.None, transactionStatusCache, clientMessenger,
      objectPointer, readType, retrieveLockedTransaction, readUUID)
  }
  
  test("Fail with invalid object") {
    val m = new TMessenger
    val r = mkReader(m)
    val nrev = ObjectRevision(new UUID(0,1))
    val nrev2 = ObjectRevision(new UUID(0,2))
    
    val ts = HLCTimestamp.now
    val readTime = HLCTimestamp(ts.asLong - 100)
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, readTime, Right(read.ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, readTime, Left(ObjectReadError.InvalidLocalPointer)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, readTime, Left(ObjectReadError.ObjectMismatch)))
    
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o match {
      case Left(err: InvalidObject) => err.pointer should be (ptr)
      case _ => fail("bah")
    }
  }
  
  test("Fail with corrupted object") {
    val m = new TMessenger
    val r = mkReader(m)
    val nrev = ObjectRevision(new UUID(0,1))
    val nrev2 = ObjectRevision(new UUID(0,2))
    
    val ts = HLCTimestamp.now
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, ts, Right(read.ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, ts, Left(ObjectReadError.CorruptedObject)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, ts, Left(ObjectReadError.CorruptedObject)))
    
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o match {
      case Left(err: CorruptedObject) => err.pointer should be (ptr)
      case _ => fail("bah")
    }
  }
  
  test("Succeed with errors") {
    val m = new TMessenger
    val r = mkReader(m)
    val nrev = ObjectRevision(new UUID(0,1))
    val nrev2 = ObjectRevision(new UUID(0,2))
    val ts = HLCTimestamp.now
    val readTime = HLCTimestamp(ts.asLong - 100)
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, readTime, Right(read.ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, readTime, Left(ObjectReadError.InvalidLocalPointer)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, readTime, Right(read.ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, readTime, Right(read.ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
//    o match {
//      case Left(_) => 
//      case Right((ds:DataObjectState, o)) => 
//        println(s"ptr(${ds.pointer}), rev(${ds.revision}), ref(${ds.refcount}), ts(${ds.timestamp}), data(${com.ibm.aspen.util.db2string(ds.data)})")
//        println(s"ptr(${ptr}), rev(${nrev2}), ref(${ref}), ts(${ts}), data(${com.ibm.aspen.util.db2string(odata)})")
//    }
    
    o should be (Right(DataObjectState(ptr, nrev2, ref, ts, readTime, 5, odata)))
  }
  
  test("Ignore old revisions") {
    val m = new TMessenger
    val r = mkReader(m)
    val nrev = ObjectRevision(new UUID(0,1))
    val nrev2 = ObjectRevision(new UUID(0,2))
    val ts = HLCTimestamp.now
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, ts, Right(read.ReadResponse.CurrentState(rev,   ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, ts, Right(read.ReadResponse.CurrentState(nrev,  ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, ts, Right(read.ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, ts, Right(read.ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(DataObjectState(ptr, nrev2, ref, ts, ts, 5, odata)))
  }
  
  test("Use minimum readTime") {
    val m = new TMessenger
    val r = mkReader(m)
    val nrev = ObjectRevision(new UUID(0,1))
    val nrev2 = ObjectRevision(new UUID(0,2))
    val ts = HLCTimestamp.now
    
    val minTs = HLCTimestamp(ts.asLong-100)
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, ts, Right(read.ReadResponse.CurrentState(rev,   ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, ts, Right(read.ReadResponse.CurrentState(nrev,  ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, minTs, Right(read.ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, ts, Right(read.ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(DataObjectState(ptr, nrev2, ref, ts, minTs, 5, odata)))
  }
  
  
  test("Successful read with data and locks") {
    val m = new TMessenger
    val r = mkReader(m)
    val ts = HLCTimestamp.now
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, ts, Right(read.ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, ts, Right(read.ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(DataObjectState(ptr, rev, ref, ts, ts, 5, odata)))
  }
  
  test("Successful read without data or locks") {
    val m = new TMessenger
    val r = mkReader(m, readType=MetadataOnly(), retrieveLockedTransaction=false)
    val ts = HLCTimestamp.now
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, ts, Right(read.ReadResponse.CurrentState(rev, ref, ts, 0, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, ts, Right(read.ReadResponse.CurrentState(rev, ref, ts, 0, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(MetadataObjectState(ptr, rev, ref, ts, ts)))
  }
}