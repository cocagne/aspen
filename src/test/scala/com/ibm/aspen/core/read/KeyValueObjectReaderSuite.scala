package com.ibm.aspen.core.read

import java.nio.charset.StandardCharsets
import java.util.UUID

import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.data_store.{DataStoreID, ObjectReadError, StoreKeyValueObjectContent}
import com.ibm.aspen.core.ida.{IDA, Replication}
import com.ibm.aspen.core.objects.keyvalue._
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, KeyValueObjectState, ObjectRefcount, ObjectRevision}
import org.scalatest.{Assertion, FunSuite, Matchers}

object KeyValueObjectReaderSuite {
  val pool = new UUID(0,1)
  val readUUID = new UUID(1,2)
  val objUUID = new UUID(1,3)

  val s0 = DataStoreID(pool, 0)
  val s1 = DataStoreID(pool, 1)
  val s2 = DataStoreID(pool, 2)
  val s3 = DataStoreID(pool, 3)
  val s4 = DataStoreID(pool, 4)

  val ida3 = Replication(3,2)
  val ida5 = Replication(5,3)

  val r0 = ObjectRevision(new UUID(0,0))
  val r1 = ObjectRevision(new UUID(0,1))
  val r2 = ObjectRevision(new UUID(0,2))

  val t0 = HLCTimestamp(1)
  val t1 = HLCTimestamp(2)
  val t2 = HLCTimestamp(3)

  val v0 = (r0, t0)
  val v1 = (r1, t1)
  val v2 = (r2, t2)

  val foo = "foo".getBytes(StandardCharsets.UTF_8)
  val bar = "bar".getBytes(StandardCharsets.UTF_8)
  val baz = "baz".getBytes(StandardCharsets.UTF_8)

  val kfoo = Key("foo")
  val kbar = Key("bar")
  val kbaz = Key("baz")

  val ka = Key("foo")
  val a0 = Insert(ka, foo, Some(t0), Some(r0))
  val a1 = Insert(ka, bar, Some(t1), Some(r1))
  val a2 = Insert(ka, baz, Some(t2), Some(r2))

  val kb = Key("bar")
  val b0 = Insert(kb, foo, Some(t0), Some(r0))
  val b1 = Insert(kb, bar, Some(t1), Some(r1))
  val b2 = Insert(kb, baz, Some(t2), Some(r2))

  val kc = Key("baz")
  val c0 = Insert(kc, foo, Some(t0), Some(r0))
  val c1 = Insert(kc, bar, Some(t1), Some(r1))
  val c2 = Insert(kc, baz, Some(t2), Some(r2))

  val min0 = SetMin(kfoo, Some(t0), Some(r0))
  val min1 = SetMin(kbar, Some(t1), Some(r1))
  val min2 = SetMin(kbaz, Some(t2), Some(r2))

  val max0 = SetMax(kfoo, Some(t0), Some(r0))
  val max1 = SetMax(kbar, Some(t1), Some(r1))
  val max2 = SetMax(kbaz, Some(t2), Some(r2))

  val left0 = SetLeft(foo, Some(t0), Some(r0))
  val left1 = SetLeft(bar, Some(t1), Some(r1))
  val left2 = SetLeft(baz, Some(t2), Some(r2))

  val right0 = SetRight(foo, Some(t0), Some(r0))
  val right1 = SetRight(bar, Some(t1), Some(r1))
  val right2 = SetRight(baz, Some(t2), Some(r2))

  class TestReader(val ida: IDA) extends KeyValueObjectReader(
    false, KeyValueObjectPointer(objUUID, pool, None, ida, Array()), new UUID(0,0)) {

    def err(store: Int, e: ObjectReadError.Value): Unit = {
      receiveReadResponse(ReadResponse(DataStoreID(pool, store.asInstanceOf[Byte]), readUUID, HLCTimestamp.Zero, Left(e)))
    }

    def ok(store: Int,
           oversion: (ObjectRevision, HLCTimestamp),
           locks: Set[ObjectRevision],
           ops: KeyValueOperation*): Unit = {

      val odata = if (ops.isEmpty) None else {
        val encodedOps = KeyValueOperation.encode(ops.toList, ida)(store)

        Some(StoreKeyValueObjectContent().update(encodedOps, oversion._1, oversion._2).encode())
      }

      val cs = ReadResponse.CurrentState(
        oversion._1,
        ObjectRefcount(0, 0),
        oversion._2,
        odata.map(_.size).getOrElse(0),
        odata,
        locks.map(r => r.lastUpdateTxUUID))

      receiveReadResponse(ReadResponse(DataStoreID(pool, store.asInstanceOf[Byte]), readUUID, HLCTimestamp.Zero, Right(cs)))
    }
  }

  object TestReader {
    def apply(ida: IDA): TestReader = new TestReader(ida)
  }
}

class KeyValueObjectReaderSuite extends FunSuite with Matchers {
  import KeyValueObjectReaderSuite._

  test("Resolve empty object") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set())
    r.result should be (None)
    r.ok(1, v0, Set())
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set())
  }

  test("Resolve single kv pair, simple") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set(), a0)
    r.result should be (None)
    r.ok(1, v0, Set(), a0)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set())
    r.result.get match {
      case Left(_) => fail()
      case Right(os) => os.asInstanceOf[KeyValueObjectState].contents.head._2 should be (Value(ka, foo, t0, r0))
    }
  }

  test("Resolve single kv pair, upreved") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set(), a0)
    r.result should be (None)
    r.rereadCandidates.keySet should be (Set())
    r.ok(1, v0, Set(), a1)
    r.result should be (None)
    r.rereadCandidates.keySet should be (Set(s0))
    r.ok(2, v0, Set(), a1)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set(s0))
    r.result.get match {
      case Left(_) => fail()
      case Right(os) => os.asInstanceOf[KeyValueObjectState].contents.head._2 should be (Value(ka, bar, t1, r1))
    }
  }

  test("Resolve multiple kv pair, simple") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set(), a0, b0)
    r.result should be (None)
    r.ok(1, v0, Set(), b0, a0)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set())
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c(ka) should be (Value(ka, foo, t0, r0))
        c(kb) should be (Value(kb, foo, t0, r0))
    }
  }

  test("Resolve three kv pair, simple") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set(), a0, b0, c1)
    r.result should be (None)
    r.ok(1, v0, Set(), c1, b0, a0)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set())
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c(ka) should be (Value(ka, foo, t0, r0))
        c(kb) should be (Value(kb, foo, t0, r0))
        c(kc) should be (Value(kc, bar, t1, r1))
    }
  }

  test("Resolve three kv pair, intermixed and partial") {
    val r = TestReader(ida3)
    r.ok(0, v1, Set(), a0, b0)
    r.result should be (None)
    r.ok(1, v0, Set(), c0, b0)
    r.result should be (None)
    r.ok(2, v1, Set(), a0, b0, c0)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set(s1))
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c(ka) should be (Value(ka, foo, t0, r0))
        c(kb) should be (Value(kb, foo, t0, r0))
        c(kc) should be (Value(kc, foo, t0, r0))
    }
  }

  test("Resolve multiple kv pair, upreved intermixed") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set(), a0, b1)
    r.result should be (None)
    r.rereadCandidates.keySet should be (Set())
    r.ok(1, v0, Set(), a1, b0)
    r.result should be (None)
    r.rereadCandidates.keySet.subsetOf(Set(s0, s1)) should be (true)
    r.ok(2, v0, Set(), a1, b1)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set(s0, s1))
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c(ka) should be (Value(ka, bar, t1, r1))
        c(kb) should be (Value(kb, bar, t1, r1))
    }
  }

  test("Resolve multiple kv pair, upreved intermixed 5-way") {
    val r = TestReader(ida5)
    r.ok(0, v0, Set(), a0, b1, c0)
    r.result should be (None)
    r.ok(1, v0, Set(), a1, b0, c1)
    r.result should be (None)
    r.ok(2, v0, Set(), a1, b1, c2)
    r.result should be (None)
    r.ok(3, v0, Set(), a1, b0, c2)
    r.result should be (None)
    r.ok(4, v0, Set(), b1, c2)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set(s0, s1, s3))
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c(ka) should be (Value(ka, bar, t1, r1))
        c(kb) should be (Value(kb, bar, t1, r1))
        c(kc) should be (Value(kc, baz, t2, r2))
    }
  }

  test("Resolve multiple kv pair, upreved intermixed 5-way with deletion") {
    val r = TestReader(ida5)
    r.ok(0, v0, Set(), a0, c0)
    r.result should be (None)
    r.ok(1, v0, Set(), a1, c1)
    r.result should be (None)
    r.ok(2, v0, Set(), a1, b1, c2)
    r.result should be (None)
    r.ok(3, v0, Set(), a1, b0, c2)
    r.result should be (None)
    r.ok(4, v0, Set(), c2)
    r.result should not be (None)
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c.size should be (2)
        c(ka) should be (Value(ka, bar, t1, r1))
        c(kc) should be (Value(kc, baz, t2, r2))
    }
  }

  test("Resolve multiple kv pair, upreved intermixed 5-way with deletion and min/max") {
    val r = TestReader(ida5)
    r.ok(0, v0, Set(), a0, c0, max1)
    r.result should be (None)
    r.ok(1, v0, Set(), a1, c1, min0, max1)
    r.result should be (None)
    r.ok(2, v0, Set(), a1, b1, c2, min0, max0)
    r.result should be (None)
    r.ok(3, v0, Set(), a1, b0, c2, min0)
    r.result should be (None)
    r.ok(4, v0, Set(), c2, max1)
    r.result should not be (None)
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val kvoss = os.asInstanceOf[KeyValueObjectState]
        val c = kvoss.contents
        c.size should be (2)
        c(ka) should be (Value(ka, bar, t1, r1))
        c(kc) should be (Value(kc, baz, t2, r2))
        kvoss.minimum should be (Some(KeyValueObjectState.Min(kfoo, r0, t0)))
        kvoss.maximum should be (Some(KeyValueObjectState.Max(kbar, r1, t1)))

    }
  }

  def tdeleted(op0: KeyValueOperation): Assertion = {
    val r = TestReader(ida5)
    r.ok(0, v0, Set())
    r.result should be (None)
    r.ok(1, v0, Set(), op0)
    r.result should be (None)
    r.ok(2, v0, Set())
    r.result should be (None)
    r.ok(3, v0, Set())
    r.result should not be (None)
  }

  test("Resolve deleted/non-restorable min") {
    tdeleted(min0)
  }
  test("Resolve deleted/non-restorable max") {
    tdeleted(max0)
  }
  test("Resolve deleted/non-restorable left") {
    tdeleted(left0)
  }
  test("Resolve deleted/non-restorable right") {
    tdeleted(right0)
  }
  test("Resolve deleted/non-restorable kv pair") {
    tdeleted(a0)
  }

  def tdeletedWithLock(op0: KeyValueOperation): Assertion = {
    val r = TestReader(ida5)
    r.ok(0, v0, Set(r0))
    r.result should be (None)
    r.ok(1, v0, Set(), op0)
    r.result should be (None)
    r.ok(2, v0, Set())
    r.result should be (None)
    r.ok(3, v0, Set())
    r.result should be (None)
    r.ok(0, v0, Set()) // discard lock
    r.result should not be (None)
  }

  test("Resolve deleted/non-restorabler min with lock") {
    tdeletedWithLock(min0)
  }
  test("Resolve deleted/non-restorabler max with lock") {
    tdeletedWithLock(max0)
  }
  test("Resolve deleted/non-restorabler left with lock") {
    tdeletedWithLock(left0)
  }
  test("Resolve deleted/non-restorabler right with lock") {
    tdeletedWithLock(right0)
  }
  test("Resolve deleted/non-restorabler kv pair with lock") {
    tdeletedWithLock(a0)
  }
}
