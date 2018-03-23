package com.ibm.aspen.core.read

import org.scalatest._
import scala.concurrent.duration._
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.network.ClientSideReadMessenger
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.read
import scala.concurrent.Await
import com.ibm.aspen.core.transaction.TransactionDescription
import java.nio.ByteBuffer
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.DataObjectPointer
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.core.objects.keyvalue.Value
import com.ibm.aspen.core.objects.MetadataObjectState
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.data_store.ObjectReadError

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
    
    def send(toStore: DataStoreID, message: read.Read): Unit = mlist = (toStore, message) :: mlist
    
    def clear() = mlist = Nil
    
    val clientId = BaseReadDriverSuite.client
  }
}

class BaseReadDriverSuite  extends AsyncFunSuite with Matchers {
  import BaseReadDriverSuite._
  
  def mkReader(clientMessenger: ClientSideReadMessenger,
               objectPointer: ObjectPointer = ptr,
               readType: ReadType = FullObject(),
               retrieveLockedTransaction: Boolean = true,
               readUUID:UUID = readUUID) = new BaseReadDriver(clientMessenger, objectPointer, readType, retrieveLockedTransaction, readUUID)
  
  test("Fail with invalid object") {
    val m = new TMessenger
    val r = mkReader(m)
    val nrev = ObjectRevision(new UUID(0,1))
    val nrev2 = ObjectRevision(new UUID(0,2))
    
    val ts = HLCTimestamp.now
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Left(ObjectReadError.InvalidLocalPointer)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, Left(ObjectReadError.ObjectMismatch)))
    
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
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Left(ObjectReadError.CorruptedObject)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, Left(ObjectReadError.CorruptedObject)))
    
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
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Left(ObjectReadError.InvalidLocalPointer)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, Right(read.ReadResponse.CurrentState(nrev2, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(nrev2, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
//    o match {
//      case Left(_) => 
//      case Right((ds:DataObjectState, o)) => 
//        println(s"ptr(${ds.pointer}), rev(${ds.revision}), ref(${ds.refcount}), ts(${ds.timestamp}), data(${com.ibm.aspen.util.db2string(ds.data)})")
//        println(s"ptr(${ptr}), rev(${nrev2}), ref(${ref}), ts(${ts}), data(${com.ibm.aspen.util.db2string(odata)})")
//    }
    
    o should be (Right(((DataObjectState(ptr, nrev2, ref, ts, 5, odata), noLocks))))
  }
  
  test("Ignore old revisions") {
    val m = new TMessenger
    val r = mkReader(m)
    val nrev = ObjectRevision(new UUID(0,1))
    val nrev2 = ObjectRevision(new UUID(0,2))
    val ts = HLCTimestamp.now
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Right(read.ReadResponse.CurrentState(nrev, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, Right(read.ReadResponse.CurrentState(nrev2, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(nrev2, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(((DataObjectState(ptr, nrev2, ref, ts, 5, odata), noLocks))))
  }
  
  test("Ignore mismatching update UUIDs for key-value objects") {
    val m = new TMessenger
    val r = mkReader(m, objectPointer=kvptr)
    val nrev = ObjectRevision(new UUID(0,1))
    val nrev2 = ObjectRevision(new UUID(0,2))
    val ts = HLCTimestamp.now
    
    val min = Key(List[Byte](1,2,3,4).toArray)
    val kvos = new KeyValueObjectState(kvptr, nrev2, ref, ts, 5, Some(min), None, None, None, Map())
    
    val enc = KeyValueObjectCodec.encode(ida, kvos)
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(nrev, Set[UUID](new UUID(0,0)), ref, ts, 5, None, Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Right(read.ReadResponse.CurrentState(nrev, Set[UUID](new UUID(0,0), new UUID(1,1)), ref, ts, 5, None, Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, Right(read.ReadResponse.CurrentState(nrev2, Set[UUID](new UUID(1,1), new UUID(2,2)), ref, ts, 5, Some(enc(0)), Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(nrev2, Set[UUID](new UUID(2,2), new UUID(1,1)), ref, ts, 5, Some(enc(0)), Nil))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(((kvos, noLocks))))
  }
  
  test("Successful read with data and locks") {
    val m = new TMessenger
    val r = mkReader(m)
    val ts = HLCTimestamp.now
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Right(read.ReadResponse.CurrentState(rev, Set[UUID](), ref, ts, 5, Some(odata), Nil))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(((DataObjectState(ptr, rev, ref, ts, 5, odata), noLocks))))
  }
  
  test("Successful read without data or locks") {
    val m = new TMessenger
    val r = mkReader(m, readType=MetadataOnly(), retrieveLockedTransaction=false)
    val ts = HLCTimestamp.now
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, Set[UUID](), ref, ts, 0, Some(odata), Nil))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Right(read.ReadResponse.CurrentState(rev, Set[UUID](), ref, ts, 0, Some(odata), Nil))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(((MetadataObjectState(ptr, rev, ref, ts), None))))
  }
}