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
import com.ibm.aspen.core.network.Client
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.read
import scala.concurrent.Await
import com.ibm.aspen.core.transaction.TransactionDescription
import java.nio.ByteBuffer

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
  
  val ptr = ObjectPointer(objUUID, poolUUID, None, ida, (sp0 :: sp1 :: sp2 :: Nil).toArray)
  val rev = ObjectRevision(0,4)
  val ref = ObjectRefcount(1,1)
  
  val odata = ByteBuffer.wrap(List[Byte](1,2,3,4).toArray)
  
  val noLocks = List[(DataStoreID,TransactionDescription)]()
  
  val client = Client(cliUUID)
  
  class TMessenger extends ClientSideReadMessenger {
    var mlist = List[(DataStoreID,read.Message)]()
    
    def send(toStore: DataStoreID, message: read.Message): Unit = mlist = (toStore, message) :: mlist
    
    def clear() = mlist = Nil
    
    val client = BaseReadDriverSuite.client
  }
}

class BaseReadDriverSuite  extends AsyncFunSuite with Matchers {
  import BaseReadDriverSuite._
  
  def mkReader(clientMessenger: ClientSideReadMessenger,
               objectPointer: ObjectPointer = ptr,
               retrieveObjectData: Boolean = true,
               retrieveLockedTransaction: Boolean = true,
               readUUID:UUID = readUUID) = new BaseReadDriver(objectPointer, retrieveObjectData, retrieveLockedTransaction, clientMessenger, readUUID)
  
  test("Fail with too many errors") {
    val m = new TMessenger
    val r = mkReader(m)
    val nrev = ObjectRevision(1,4)
    val nrev2 = ObjectRevision(2,4)
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, ref, Some(odata), None))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Left(ReadError.InvalidLocalPointer)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, Left(ReadError.NoResponse)))
    
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Left(ThresholdError(Map(
        (ds0 -> None), 
        (ds1 -> Some(ReadError.InvalidLocalPointer)),
        (ds2 -> Some(ReadError.NoResponse))))))
  }
  
  test("Succeed with errors") {
    val m = new TMessenger
    val r = mkReader(m)
    val nrev = ObjectRevision(1,4)
    val nrev2 = ObjectRevision(2,4)
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, ref, Some(odata), None))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Left(ReadError.InvalidLocalPointer)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, Right(read.ReadResponse.CurrentState(nrev2, ref, Some(odata), None))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(nrev2, ref, Some(odata), None))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(ObjectState(ptr, nrev2, ref, Some(odata), Some(noLocks))))
  }
  
  test("Ignore old revisions") {
    val m = new TMessenger
    val r = mkReader(m)
    val nrev = ObjectRevision(1,4)
    val nrev2 = ObjectRevision(2,4)
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, ref, Some(odata), None))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Right(read.ReadResponse.CurrentState(nrev, ref, Some(odata), None))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds2, readUUID, Right(read.ReadResponse.CurrentState(nrev2, ref, Some(odata), None))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(nrev2, ref, Some(odata), None))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(ObjectState(ptr, nrev2, ref, Some(odata), Some(noLocks))))
  }
  
  test("Successful read with data and locks") {
    val m = new TMessenger
    val r = mkReader(m)
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, ref, Some(odata), None))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Right(read.ReadResponse.CurrentState(rev, ref, Some(odata), None))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(ObjectState(ptr, rev, ref, Some(odata), Some(noLocks))))
  }
  
  test("Successful read without data or locks") {
    val m = new TMessenger
    val r = mkReader(m, retrieveObjectData=false, retrieveLockedTransaction=false)
    
    r.receiveReadResponse(read.ReadResponse(ds0, readUUID, Right(read.ReadResponse.CurrentState(rev, ref, Some(odata), None))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(read.ReadResponse(ds1, readUUID, Right(read.ReadResponse.CurrentState(rev, ref, Some(odata), None))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)
    
    o should be (Right(ObjectState(ptr, rev, ref, None, None)))
  }
}