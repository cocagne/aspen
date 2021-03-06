package com.ibm.aspen.core.read

import java.util.UUID

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.{DataStoreID, ObjectReadError}
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.objects.{DataObjectPointer, ObjectRefcount, ObjectRevision, ObjectState}
import org.scalatest.{FunSuite, Matchers}

object DataObjectReaderSuite {
  class TestReader(pointer: DataObjectPointer)
    extends BaseObjectReader[DataObjectPointer, DataObjectStoreState](false, pointer, new UUID(0,0)) {

    var rstates: Option[List[StoreState]] = None

    override protected def createObjectState(storeId:DataStoreID, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): DataObjectStoreState = {
      new DataObjectStoreState(storeId, cs.revision, cs.refcount, cs.timestamp, readTime, cs.sizeOnStore, cs.objectData)
    }

    override protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                                         readTime: HLCTimestamp, matchingStoreStates: List[DataObjectStoreState],
                                         allStoreStates: List[DataObjectStoreState]): ObjectState = {
      rstates = Some(matchingStoreStates)
      null
    }
  }
  object TestReader {
    def apply(width: Int, threshold: Int): TestReader = {

        val ida = Replication(width, threshold)

        new TestReader(DataObjectPointer(objUUID, pool, None, ida, Array()))
    }
  }
  val pool = new UUID(0,1)
  val readUUID = new UUID(1,2)
  val objUUID = new UUID(1,3)

  val s0 = DataStoreID(pool, 0)
  val s1 = DataStoreID(pool, 1)
  val s2 = DataStoreID(pool, 2)
  val s3 = DataStoreID(pool, 3)
  val s4 = DataStoreID(pool, 4)

  val r0 = ObjectRevision(new UUID(0,0))
  val r1 = ObjectRevision(new UUID(0,1))
  val r2 = ObjectRevision(new UUID(0,2))

  val t0 = HLCTimestamp(1)
  val t1 = HLCTimestamp(2)
  val t3 = HLCTimestamp(3)

  def err(store: Int, e: ObjectReadError.Value): ReadResponse = {
    ReadResponse(DataStoreID(pool, store.asInstanceOf[Byte]), readUUID, HLCTimestamp.Zero, Left(e))
  }

  def ok(store: Int, rev: ObjectRevision, ts: HLCTimestamp): ReadResponse = {
    val cs = ReadResponse.CurrentState(rev, ObjectRefcount(0,0), ts, 0, None, Set())
    ReadResponse(DataStoreID(pool, store.asInstanceOf[Byte]), readUUID, HLCTimestamp.Zero, Right(cs))
  }


}

class DataObjectReaderSuite extends FunSuite with Matchers {
  import DataObjectReaderSuite._
/*
final case class ReadResponse(
    fromStore: DataStoreID,
    readUUID: UUID,
    readTime: HLCTimestamp,
    result: Either[ObjectReadError.Value, ReadResponse.CurrentState]) extends Message

object ObjectReadError extends Enumeration {
  val InvalidLocalPointer = Value("InvalidLocalPointer")
  val ObjectMismatch = Value("ObjectMismatch")
  val CorruptedObject = Value("CorruptedObject")

object ReadResponse {
  case class CurrentState(
      revision: ObjectRevision,
      refcount: ObjectRefcount,
      timestamp: HLCTimestamp,
      sizeOnStore: Int,
      objectData: Option[DataBuffer],
      lockedWriteTransactions: Set[UUID])

      */
  test("Simple Success") {
    val r = TestReader(3,2)
    r.receiveReadResponse(ok(0, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(0, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(1, r0, t0))
    r.rstates.nonEmpty should be (true)
  }

  test("Simple InvalidLocalPointer Failure") {
    val r = TestReader(3,2)
    r.receiveReadResponse(ok(0, r0, t0))
    r.result should be (None)
    r.receiveReadResponse(err(1, ObjectReadError.InvalidLocalPointer))
    r.result should be (None)
    r.receiveReadResponse(err(2, ObjectReadError.InvalidLocalPointer))
    r.result should be (Some(Left(ObjectReadError.InvalidLocalPointer)))
  }

  test("Simple Mismatch Failure") {
    val r = TestReader(3,2)
    r.receiveReadResponse(ok(0, r0, t0))
    r.result should be (None)
    r.receiveReadResponse(err(1, ObjectReadError.ObjectMismatch))
    r.result should be (None)
    r.receiveReadResponse(err(2, ObjectReadError.ObjectMismatch))
    r.result should be (Some(Left(ObjectReadError.InvalidLocalPointer)))
  }

  test("Simple Corruption Failure") {
    val r = TestReader(3,2)
    r.receiveReadResponse(ok(0, r0, t0))
    r.result should be (None)
    r.receiveReadResponse(err(1, ObjectReadError.CorruptedObject))
    r.result should be (None)
    r.receiveReadResponse(err(2, ObjectReadError.CorruptedObject))
    r.result should be (Some(Left(ObjectReadError.CorruptedObject)))
  }

  test("Mixed error response failure") {
    val r = TestReader(3,2)
    r.receiveReadResponse(ok(0, r0, t0))
    r.result should be (None)
    r.receiveReadResponse(err(1, ObjectReadError.ObjectMismatch))
    r.result should be (None)
    r.receiveReadResponse(err(2, ObjectReadError.InvalidLocalPointer))
    r.result should be (Some(Left(ObjectReadError.InvalidLocalPointer)))
  }

  test("Use highest revision") {
    val r = TestReader(5,3)
    r.receiveReadResponse(ok(0, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(1, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(2, r1, t1))
    r.rstates should be (None)
    r.rereadCandidates.keySet should be (Set(s0,s1))
    //rereads should be (Set(0,1))
    r.receiveReadResponse(ok(3, r0, t0))
    r.rstates should be (None)
    r.rereadCandidates.keySet should be (Set(s0,s1,s3))
    r.receiveReadResponse(ok(0, r1, t1))
    r.rstates should be (None)
    r.receiveReadResponse(ok(1, r1, t1))
    r.rereadCandidates.keySet should be (Set(s3))
    r.rstates.nonEmpty should be (true)
  }

  test("Resolve with errors") {
    val r = TestReader(5,3)
    r.receiveReadResponse(err(0, ObjectReadError.CorruptedObject))
    r.rstates should be (None)
    r.receiveReadResponse(ok(1, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(2, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(err(3, ObjectReadError.CorruptedObject))
    r.rstates should be (None)
    r.receiveReadResponse(ok(4, r0, t0))
    r.rstates.nonEmpty should be (true)
  }

  test("Rereads override previous state") {
    val r = TestReader(5,3)
    r.receiveReadResponse(ok(0, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(1, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(2, r1, t1))
    r.rstates should be (None)
    //rereads should be (Set(0,1))
    r.receiveReadResponse(ok(0, r1, t1))
    r.rstates should be (None)
    r.receiveReadResponse(ok(1, r1, t1))
    r.rstates.nonEmpty should be (true)
  }
}
