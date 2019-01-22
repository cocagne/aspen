package com.ibm.aspen.core.data_store

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.aspen.base.ObjectReader
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.allocation.{DataAllocationOptions, KeyValueAllocationOptions, ObjectAllocationRevisionGuard}
import com.ibm.aspen.core.ida.Replication
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.objects._
import com.ibm.aspen.core.objects.keyvalue._
import com.ibm.aspen.core.read.{OpportunisticRebuild, ReadDriver}
import com.ibm.aspen.core.transaction._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object RebuildSuite {
  val objUUID = new UUID(0,1)
  val poolUUID = new UUID(0,2)
  val txUUID = new UUID(0,3)
  val allocUUID = new UUID(0,4)
  val allocObj = DataObjectPointer(new UUID(0,5), poolUUID, None, Replication(3,2), new Array[StorePointer](0))
  val allocRev = ObjectRevision(allocUUID)
  val allocTs = HLCTimestamp(0)
  val allocRef = ObjectRefcount(0,1)

  val allocMeta = ObjectMetadata(allocRev, allocRef, allocTs)

  val uuid2 = new UUID(0,6)
  val rev2 = ObjectRevision(uuid2)
  val ref2 = ObjectRefcount(5,5)
  val ts2 = HLCTimestamp(2)

  val meta2 = ObjectMetadata(rev2, ref2, ts2)

  val txrev = ObjectRevision(txUUID)
  val txts = HLCTimestamp(1)

  val txmeta = ObjectMetadata(txrev, allocRef, txts)

  def mkDataObjPtr(sp:StorePointer) = DataObjectPointer(objUUID, poolUUID, None, Replication(3,2), (sp::Nil).toArray)

  def mkKVObjPtr(sp:StorePointer) = KeyValueObjectPointer(objUUID, poolUUID, None, Replication(3,2), (sp::Nil).toArray)

  def mktxd(reqs: List[TransactionRequirement], txdUUID:UUID=txUUID): TransactionDescription = {
    TransactionDescription(txdUUID, txts.asLong, allocObj, 0, reqs, Nil)
  }

  def mklu(objectPointer: ObjectPointer, content: DataBuffer = DataBuffer.Empty): List[LocalUpdate] = {
    List(LocalUpdate(objectPointer.uuid, content))
  }

  class TReader(val o: ObjectState) extends ObjectReader {
    def readObject(pointer:DataObjectPointer, readStrategy: Option[ReadDriver.Factory], disableOpportunisticRebuild:Boolean): Future[DataObjectState] = {
      o match {
        case d: DataObjectState => Future.successful(d)
        case _ => Future.failed(new Exception("Wrong object type"))
      }
    }

    def readObject(pointer:KeyValueObjectPointer, readStrategy: Option[ReadDriver.Factory], disableOpportunisticRebuild:Boolean): Future[KeyValueObjectState] = {
      o match {
        case d: KeyValueObjectState => Future.successful(d)
        case _ => Future.failed(new Exception("Wrong object type"))
      }
    }

    def readSingleKey(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = Future.failed(new Exception("Meh"))

    def readLargestKeyLessThan(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = Future.failed(new Exception("Meh"))

    def readLargestKeyLessThanOrEqualTo(pointer: KeyValueObjectPointer, key: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = Future.failed(new Exception("Meh"))

    def readKeyRange(pointer: KeyValueObjectPointer, minimum: Key, maximum: Key, comparison: KeyOrdering): Future[KeyValueObjectState] = Future.failed(new Exception("Meh"))
  }

  class DelayedReader(o: ObjectState)(implicit ec: ExecutionContext) extends TReader(o) {

    var pallow = Promise[Unit]()

    var pread = Promise[Unit]()

    def completeWhenReadStarted: Future[Unit] = pread.future

    def allowRead(): Unit = pallow.success(())

    override def readObject(pointer:DataObjectPointer, readStrategy: Option[ReadDriver.Factory], disableOpportunisticRebuild:Boolean): Future[DataObjectState] = {
      if (!pread.isCompleted)
        pread.success(())
      pallow.future.flatMap(_ => super.readObject(pointer, readStrategy, disableOpportunisticRebuild))
    }

    override def readObject(pointer:KeyValueObjectPointer, readStrategy: Option[ReadDriver.Factory], disableOpportunisticRebuild:Boolean): Future[KeyValueObjectState] = {
      if (!pread.isCompleted)
        pread.success(())
      pallow.future.flatMap(_ => super.readObject(pointer, readStrategy, disableOpportunisticRebuild))
    }
  }
}

class RebuildSuite extends AsyncFunSuite with Matchers {

  // Reuse the constants & helper functions from DataObjectTransactionSuite
  import RebuildSuite._

  def newStore: DataStoreFrontend = new DataStoreFrontend(DataObjectTransactionSuite.storeId,
    new MemoryOnlyDataStoreBackend()(ExecutionContext.Implicits.global), Nil, Nil)

  def initDataObject(icontent: DataBuffer): Future[(DataStoreFrontend, DataObjectPointer)] = {
    val ds = newStore

    ds.allocate(objUUID, new DataAllocationOptions, None, allocRef, icontent, allocTs, allocUUID, ObjectAllocationRevisionGuard(allocObj, allocRev)) flatMap {
      case Right(ars0) => Future.successful((ds, mkDataObjPtr(ars0.storePointer)))
      case Left(_) => throw new Exception("Returned failure instead of store pointer")
    }
  }

  def initKVObject(omin: Option[KeyValueObjectState.Min]=None, omap: Option[Map[Key,Value]]=None): Future[(DataStoreFrontend, KeyValueObjectPointer)] = {
    val ds = newStore

    var ops: List[KeyValueOperation] = Nil

    omin.foreach { m =>
      ops = SetMin(m.key, Some(m.timestamp), Some(m.revision)) :: ops
    }

    omap.foreach { m =>
      m.values.foreach { v =>
        ops = Insert(v.key, v.value, Some(v.timestamp), Some(v.revision)) :: ops
      }
    }

    val data = if (ops.isEmpty) DataBuffer.Empty else KeyValueOperation.encode(ops, Replication(3,2))(0)

    ds.allocate(objUUID, new KeyValueAllocationOptions, None, allocRef, data, allocTs, allocUUID, ObjectAllocationRevisionGuard(allocObj, allocRev)) flatMap {
      case Right(ars0) => Future.successful((ds, mkKVObjPtr(ars0.storePointer)))
      case Left(_) => throw new Exception("Returned failure instead of store pointer")
    }
  }
  /*final case class OpportunisticRebuild(
    toStore: DataStoreID,
    fromClient: ClientID,
    pointer: ObjectPointer,
    revision: ObjectRevision,
    refcount: ObjectRefcount,
    timestamp: HLCTimestamp,
    data: DataBuffer) extends Message {
}*/

  test("Successful opportunistic rebuild") {
    val d2 = DataBuffer(Array[Byte](1,2))
    for {
      (ds, ptr) <- initDataObject(DataBuffer.Empty)
      i <- ds.getObject(ptr)
      o = OpportunisticRebuild(ds.storeId, ClientID(objUUID), ptr, rev2, ref2, ts2, d2)
      _ <- ds.opportunisticRebuild(o)
      f <- ds.getObject(ptr)
    } yield {
      i should be (Right((allocMeta, DataBuffer.Empty, List(), Set())))
      f should be (Right((meta2, d2, List(), Set())))
    }
  }

  test("Update refcount but not data in opportunistic rebuild") {
    val d2 = DataBuffer(Array[Byte](1,2))
    for {
      (ds, ptr) <- initDataObject(DataBuffer.Empty)
      i <- ds.getObject(ptr)
      o = OpportunisticRebuild(ds.storeId, ClientID(objUUID), ptr, rev2, ref2, allocTs, d2)
      _ <- ds.opportunisticRebuild(o)
      f <- ds.getObject(ptr)
    } yield {
      i should be (Right((allocMeta, DataBuffer.Empty, List(), Set())))
      f should be (Right((allocMeta.copy(refcount = ref2), DataBuffer.Empty, List(), Set())))
    }
  }

  test("Unsuccessful opportunistic rebuild") {
    val d2 = DataBuffer(Array[Byte](1,2))
    for {
      (ds, ptr) <- initDataObject(DataBuffer.Empty)
      i <- ds.getObject(ptr)
      o = OpportunisticRebuild(ds.storeId, ClientID(objUUID), ptr, rev2, allocRef, allocTs, d2)
      _ <- ds.opportunisticRebuild(o)
      f <- ds.getObject(ptr)
    } yield {
      i should be (Right((allocMeta, DataBuffer.Empty, List(), Set())))
      f should be (Right((allocMeta, DataBuffer.Empty, List(), Set())))
    }
  }

  test("KeyValue opportunistic rebuild") {
    val minKey = Key(Array[Byte](1,2))
    val min = KeyValueObjectState.Min(minKey, rev2, ts2)
    val k1 = Key(Array[Byte](3,4))
    val v = Value(k1, Array[Byte](5,6), ts2, rev2)
    val m = Map(k1->v)
    val imin = KeyValueObjectState.Min(minKey, allocRev, allocTs)
    val imap = Map(k1->Value(k1, Array[Byte](), allocTs, allocRev))
    for {
      (ds, ptr) <- initKVObject(Some(imin), Some(imap))
      i <- ds.getObject(ptr)
      os = new KeyValueObjectState(ptr, rev2, ref2, ts2, ts2, Some(min), None, None, None, m)
      o = OpportunisticRebuild(ds.storeId, ClientID(objUUID), ptr, rev2, ref2, ts2, os.getRebuildDataForStore(ds.storeId).get)
      _ <- ds.opportunisticRebuild(o)
      f <- ds.getObject(ptr)
    } yield {
      i match {
        case Left(_) => fail()
        case Right((meta, data, _, _)) =>
          meta should be (allocMeta)
          val kvoss = StoreKeyValueObjectContent(data)
          kvoss.minimum match {
            case None => fail()
            case Some(m2) =>
              m2.key should be (imin.key)
              m2.timestamp should be (imin.timestamp)
              m2.revision should be (imin.revision)
          }
          kvoss.maximum should be (None)
          kvoss.left should be (None)
          kvoss.right should be (None)
          kvoss.idaEncodedContents should be (imap)
      }
      f match {
        case Left(_) => fail()
        case Right((meta, data, _, _)) =>
          meta should be (meta2)
          val kvoss = StoreKeyValueObjectContent(data)
          kvoss.minimum match {
            case None => fail()
            case Some(m2) =>
              m2.key should be (minKey)
              m2.timestamp should be (ts2)
              m2.revision should be (rev2)
          }
          kvoss.maximum should be (None)
          kvoss.left should be (None)
          kvoss.right should be (None)
          kvoss.idaEncodedContents should be (m)
      }
    }
  }

  test("Basic rebuild") {
    val d2 = DataBuffer(Array[Byte](1,2))
    for {
      (ds, ptr) <- initDataObject(DataBuffer.Empty)
      i <- ds.getObject(ptr)
      o = DataObjectState(ptr, rev2, ref2, ts2, ts2, 2, d2)
      rebuilt <- ds.rebuildObject(new TReader(o), ptr)
      f <- ds.getObject(ptr)
    } yield {
      rebuilt should be (true)
      i should be (Right((allocMeta, DataBuffer.Empty, List(), Set())))
      f should be (Right((meta2, d2, List(), Set())))
    }
  }

  test("KeyValue object rebuild") {
    val minKey = Key(Array[Byte](1,2))
    val min = KeyValueObjectState.Min(minKey, rev2, ts2)
    val k1 = Key(Array[Byte](3,4))
    val v = Value(k1, Array[Byte](5,6), ts2, rev2)
    val m = Map(k1->v)
    for {
      (ds, ptr) <- initKVObject()
      i <- ds.getObject(ptr)
      o = new KeyValueObjectState(ptr, rev2, ref2, ts2, ts2, Some(min), None, None, None, m)
      rebuilt <- ds.rebuildObject(new TReader(o), ptr)
      f <- ds.getObject(ptr)
    } yield {
      rebuilt should be (true)
      i should be (Right((allocMeta, DataBuffer.Empty, List(), Set())))
      f match {
        case Left(_) => fail()
        case Right((meta, data, _, _)) =>
          meta should be (meta2)
          val kvoss = StoreKeyValueObjectContent(data)
          kvoss.minimum match {
            case None => fail()
            case Some(m2) =>
              m2.key should be (minKey)
              m2.timestamp should be (ts2)
              m2.revision should be (rev2)
          }
          kvoss.maximum should be (None)
          kvoss.left should be (None)
          kvoss.right should be (None)
          kvoss.idaEncodedContents should be (m)
      }
    }
  }

  test("Multiple rebuilds") {
    val d2 = DataBuffer(Array[Byte](1,2))
    val d3 = DataBuffer(Array[Byte](3,4))
    for {
      (ds, ptr) <- initDataObject(DataBuffer.Empty)
      i <- ds.getObject(ptr)

      o = DataObjectState(ptr, rev2, ref2, ts2, ts2, 2, d2)
      rebuilt <- ds.rebuildObject(new TReader(o), ptr)
      f <- ds.getObject(ptr)

      o2 = DataObjectState(ptr, txrev, ref2, ts2, ts2, 2, d3)
      rebuilt2 <- ds.rebuildObject(new TReader(o2), ptr)
      f2 <- ds.getObject(ptr)
    } yield {
      rebuilt should be (true)
      i should be (Right((allocMeta, DataBuffer.Empty, List(), Set())))
      f should be (Right((meta2, d2, List(), Set())))

      rebuilt2 should be (true)
      f2 should be (Right((ObjectMetadata(txrev, ref2, ts2), d3, List(), Set())))
    }
  }

  test("Rebuild during rebuild fails") {
    val d2 = DataBuffer(Array[Byte](1,2))
    for {
      (ds, ptr) <- initDataObject(DataBuffer.Empty)
      i <- ds.getObject(ptr)
      o = DataObjectState(ptr, rev2, ref2, ts2, ts2, 2, d2)
      delayedReader = new DelayedReader(o)
      frebuild = ds.rebuildObject(delayedReader, ptr)

      // Note, this is inherently racy as we're doing to calls to async rebuildObject and we need the
      // first call to lock the object before the second call starts. It's not a very good solution but
      // we can do a copy calls go ds.getObject between them to reduce the chance of race conditions.
      _ <- ds.getObject(ptr)
      _ <- ds.getObject(ptr)
      _ <- ds.getObject(ptr)
      _ <- ds.getObject(ptr)
      _ <- ds.getObject(ptr)

      second <- ds.rebuildObject(delayedReader, ptr)

      _ = delayedReader.allowRead()
      rebuilt <- frebuild
      f <- ds.getObject(ptr)
    } yield {
      rebuilt should be (true)
      i should be (Right((allocMeta, DataBuffer.Empty, List(), Set())))
      f should be (Right((meta2, d2, List(), Set())))
      second should be (false)
    }
  }

  test("Rebuild blocks transactions") {
    val d2 = DataBuffer(Array[Byte](1,2))
    for {
      (ds, ptr) <- initDataObject(DataBuffer.Empty)
      i <- ds.getObject(ptr)
      o = DataObjectState(ptr, rev2, ref2, ts2, ts2, 2, d2)
      delayedReader = new DelayedReader(o)
      frebuild = ds.rebuildObject(delayedReader, ptr)

      _ <- delayedReader.completeWhenReadStarted

      txd = mktxd(DataUpdate(ptr, allocRev, DataUpdateOperation.Overwrite) :: Nil)
      errs <- ds.lockTransaction(txd, mklu(ptr))

      _ = delayedReader.allowRead()
      rebuilt <- frebuild
      f <- ds.getObject(ptr)
    } yield {
      rebuilt should be (true)
      i should be (Right((allocMeta, DataBuffer.Empty, List(), Set())))
      f should be (Right((meta2, d2, List(), Set())))
      errs should be (List(RebuildCollision(ptr)))
    }
  }

  test("Rebuild allows existing transactions to complete") {
    val d2 = DataBuffer(Array[Byte](1,2))
    val dtx = DataBuffer(Array[Byte](3,4,5))
    for {
      (ds, ptr) <- initDataObject(DataBuffer.Empty)

      txd = mktxd(DataUpdate(ptr, allocRev, DataUpdateOperation.Overwrite) :: Nil)
      lu = mklu(ptr, dtx)
      errs <- ds.lockTransaction(txd, lu)

      o = DataObjectState(ptr, rev2, ref2, ts2, ts2, 2, d2)
      delayedReader = new DelayedReader(o)
      frebuild = ds.rebuildObject(delayedReader, ptr)

      _ <- ds.commitTransactionUpdates(txd, lu)

      i <- ds.getObject(ptr)

      _ = delayedReader.allowRead()
      rebuilt <- frebuild
      f <- ds.getObject(ptr)
    } yield {
      rebuilt should be (true)
      i should be (Right((txmeta, dtx, List(), Set())))
      f should be (Right((meta2, d2, List(), Set())))
      errs should be (Nil)
    }
  }
}
