package com.ibm.aspen.core.data_store

import java.util.UUID

import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.objects.{ObjectRefcount, ObjectRevision, StorePointer}
import com.ibm.aspen.core.transaction.TransactionDescription

import scala.concurrent.{Future, Promise}
import scala.util.Success

object ObjectStoreState {
  val NullUUID = new UUID(0,0)
  val NullRevision = ObjectRevision(NullUUID)
  val NullRefcount = ObjectRefcount(0,0)
  val NullTimestamp = HLCTimestamp(0)
  val NullMetadata = ObjectMetadata(NullRevision, NullRefcount, NullTimestamp)
  val InvalidObjectId = StoreObjectID(NullUUID, StorePointer(-1, Array()))
}

/** Represents the current in-memory state of the object. Changes are made here first and then flushed to disk.
  *
  *  The variables contained within this class are initialized to invalid values and they do not become valid
  *  until after their corresponding Future completes. Metadata and Data are loaded independently and each has
  *  a corresponding future that completes when the variables have been initialized and are ready for use.
  *
  *  Instances of this class are maintained in memory until the set of operations referencing the object drops
  *  to zero or a read error is encountered.
  */
sealed abstract class ObjectStoreState(val frontend: DataStoreFrontend,
                                       val objectId: StoreObjectID,
                                       allocationState: Option[(ObjectMetadata, DataBuffer)]) {
  import ObjectStoreState._
  import frontend.executionContext

  // While this is non-zero, a reference to this object will be stored in frontend.loadedObjects
  private var opRefcount = 0

  var deleted: Boolean = false

  var objectRevisionReadLocks: Map[UUID, TransactionDescription] = Map()
  var objectRevisionWriteLock: Option[TransactionDescription] = None
  var objectRefcountReadLocks: Map[UUID, TransactionDescription] = Map()
  var objectRefcountWriteLock: Option[TransactionDescription] = None

  protected var meta: ObjectMetadata = NullMetadata
  protected var db: DataBuffer = DataBuffer.Empty

  private[this] val metaPromise: Promise[Either[ObjectReadError, ObjectStoreState]] = Promise()
  private[this] val dataPromise: Promise[Either[ObjectReadError, ObjectStoreState]] = Promise()

  private[this] var loadingMeta = false
  private[this] var loadingData = false
  private[this] var readError: Option[ObjectReadError] = None

  // If allocating, we have authoritative state already
  allocationState.foreach { t =>
    meta = t._1
    db = convertAllocationData(t._1, t._2)

    metaPromise.success(Right(this))
    dataPromise.success(Right(this))
  }

  def uuid: UUID = objectId.objectUUID
  def revision: ObjectRevision = meta.revision
  def refcount: ObjectRefcount = meta.refcount
  def timestamp: HLCTimestamp = meta.timestamp

  protected def dataUpdated(): Unit = {}

  protected def convertAllocationData(metadata: ObjectMetadata, allocData: DataBuffer): DataBuffer = allocData

  /** To be used only after the load future is completed */
  def loadError: Option[ObjectReadError] = readError

  /* Called by frontend when state load from the store fails. Reason for indirect call is
   * to ensure that this method executes within a synchronized block on the DataStoreFrontend
   */
  def loadFailed(err: ObjectReadError): Unit = {
    this.readError = Some(err)
    if (!metaPromise.isCompleted) metaPromise.success(Left(err))
    if (!dataPromise.isCompleted) dataPromise.success(Left(err))
  }

   /* Called by frontend when state is loaded from the store. Only set the value if it hasn't already
    * been set (commit could have been done before the read completes). Reason for indirect call is
    * to ensure that this method executes within a synchronized block on the DataStoreFrontend
    */
  def metadataLoaded(m: ObjectMetadata): Unit = if (!metaPromise.isCompleted) {
    meta = m
    metaPromise.success(Right(this))
  }

   /* Called by frontend when state is loaded from the store. Only set the value if it hasn't already
    * been set (commit could have been done before the read completes). Reason for indirect call is
    * to ensure that this method executes within a synchronized block on the DataStoreFrontend
    */
  def dataLoaded(d: DataBuffer): Unit = if (!dataPromise.isCompleted) {
    db = d
    dataUpdated()
    dataPromise.success(Right(this))
  }

  def metadata: ObjectMetadata = meta
  def metadata_=(m: ObjectMetadata): Unit = {
    meta = m

    if (!metaPromise.isCompleted)
      metaPromise.success(Right(this))
  }

  def data: DataBuffer = db
  def data_=(d: DataBuffer): Unit = {
    db = d
    dataUpdated()

    if (!dataPromise.isCompleted)
      dataPromise.success(Right(this))
  }

  def incref(): Unit = {
    if (opRefcount == 0)
      frontend.retainLoadedObject(this)
    opRefcount += 1
  }

  def decref(): Unit = {
    opRefcount -= 1
    if (opRefcount == 0)
      frontend.releaseLoadedObject(this)
    assert(opRefcount >= 0, "Refcounting screwed up!")
  }

  def getTransactionPreventingRevisionWriteLock(ignoreTxd: TransactionDescription): Option[TransactionDescription] = {
    val o = objectRevisionWriteLock match {
      case Some(txd) => if (txd.transactionUUID == ignoreTxd.transactionUUID) None else Some(txd)
      case None => None
    }

    o match {
      case Some(txd) => Some(txd)
      case None => if (objectRevisionReadLocks.isEmpty) None else Some(objectRevisionReadLocks.head._2)
    }
  }

  def getTransactionPreventingRevisionReadLock(ignoreTxd: TransactionDescription): Option[TransactionDescription] = {
    objectRevisionWriteLock match {
      case Some(txd) => if (txd.transactionUUID == ignoreTxd.transactionUUID) None else Some(txd)
      case None => None
    }
  }

  def getTransactionPreventingRefcountWriteLock(ignoreTxd: TransactionDescription): Option[TransactionDescription] = {
    val o = objectRefcountWriteLock match {
      case Some(txd) => if (txd.transactionUUID == ignoreTxd.transactionUUID) None else Some(txd)
      case None => None
    }

    o match {
      case Some(txd) => Some(txd)
      case None => if (objectRefcountReadLocks.isEmpty) None else Some(objectRefcountReadLocks.head._2)
    }
  }

  def locks: List[Lock] = {
    var ll: List[Lock] = Nil

    objectRevisionWriteLock.foreach { txd => ll = RevisionWriteLock(txd) :: ll }
    objectRefcountWriteLock.foreach { txd => ll = RefcountWriteLock(txd) :: ll }

    ll = objectRevisionReadLocks.foldLeft(ll)( (l, t) => RevisionReadLock(t._2) :: l )
    ll = objectRefcountReadLocks.foldLeft(ll)( (l, t) => RefcountReadLock(t._2) :: l )

    ll
  }

  def writeLocks: Set[UUID] = objectRefcountWriteLock.foldLeft(objectRevisionWriteLock.foldLeft(Set[UUID]())((s, l) => s + l.transactionUUID))((s, l) => s + l.transactionUUID)

  def commitMetadata(): Future[Unit] = frontend.backend.putObjectMetaData(objectId, meta)

  def commitData(): Future[Unit] = frontend.backend.putObjectData(objectId, data)

  def commitBoth(): Future[Unit] = frontend.backend.putObject(objectId, meta, data)

  def loadMetadata(): Future[Either[ObjectReadError, ObjectStoreState]] = {
    if (!metaPromise.isCompleted && !loadingMeta) {
      loadingMeta = true
      frontend.backend.getObjectMetaData(objectId) foreach {
        case Left(err) => frontend.failedToRead(this, err)
        case Right(metadata) => frontend.loadedObjectState(this, Some(metadata), None)
      }
    }
    metaPromise.future
  }


  def loadData(): Future[Either[ObjectReadError, ObjectStoreState]] = {
    if (!dataPromise.isCompleted && !loadingData) {
      loadingData = true
      frontend.backend.getObjectData(objectId) foreach {
        case Left(err) => frontend.failedToRead(this, err)
        case Right(data) => frontend.loadedObjectState(this, None, Some(data))
      }
    }
    dataPromise.future
  }

  def loadBoth(): Future[Either[ObjectReadError, ObjectStoreState]] = {
    if (!metaPromise.isCompleted && !loadingMeta && !dataPromise.isCompleted && !loadingData) {
      loadingMeta = true
      loadingData = true
      frontend.backend.getObject(objectId) map {
        case Left(err) => frontend.failedToRead(this, err)
        case Right((metadata, data)) => frontend.loadedObjectState(this, Some(metadata), Some(data))
      }
      metaPromise.future.flatMap(_ => dataPromise.future)
    }
    else if (!metaPromise.isCompleted && !loadingMeta)
      loadMetadata()
    else
      loadData()
  }
}

class DataObjectStoreState(frontend: DataStoreFrontend,
                               objectId: StoreObjectID,
                               allocationState: Option[(ObjectMetadata, DataBuffer)]) extends ObjectStoreState(
  frontend, objectId, allocationState) {

  def overwriteData(overwrite: DataBuffer): Unit = data = overwrite

  def appendData(append: DataBuffer): Unit = data = this.db.append(append)
}

class KeyValueObjectStoreState(frontend: DataStoreFrontend,
                               objectId: StoreObjectID,
                               allocationState: Option[(ObjectMetadata, DataBuffer)]) extends ObjectStoreState(
  frontend, objectId, allocationState) {

  private[this] var ocontent: Option[StoreKeyValueObjectContent] = None

  override protected def convertAllocationData(metadata: ObjectMetadata, allocData: DataBuffer): DataBuffer = {
    StoreKeyValueObjectContent().update(allocData, metadata.revision, metadata.timestamp).encode()
  }

  def kvcontent: StoreKeyValueObjectContent = ocontent match {
    case Some(x) => x
    case None =>
      val x = StoreKeyValueObjectContent(data)
      ocontent = Some(x)
      x
  }

  override protected def dataUpdated(): Unit = ocontent = None

  def update(updates: DataBuffer, txRevision: ObjectRevision, txTimestamp: HLCTimestamp): Unit = {
    val newContent = kvcontent.update(updates, txRevision, txTimestamp)
    ocontent = Some(newContent)
    db = newContent.encode()
  }

  var keyRevisionReadLocks: Map[Key, Map[UUID,TransactionDescription]] = Map()
  var keyRevisionWriteLocks: Map[Key, TransactionDescription] = Map()

  override def writeLocks: Set[UUID] = {
    keyRevisionWriteLocks.foldLeft(super.writeLocks)((s, t) => s + t._2.transactionUUID)
  }

  override def getTransactionPreventingRevisionWriteLock(ignoreTxd: TransactionDescription): Option[TransactionDescription] = {
    super.getTransactionPreventingRevisionWriteLock(ignoreTxd) match {
      case Some(txd) => Some(txd)
      case None => if (keyRevisionWriteLocks.isEmpty) {
        if (keyRevisionReadLocks.isEmpty)
          None
        else
          Some(keyRevisionReadLocks.head._2.head._2)
      } else
        Some(keyRevisionWriteLocks.head._2)
    }
  }

  override def getTransactionPreventingRevisionReadLock(ignoreTxd: TransactionDescription): Option[TransactionDescription] = {
    super.getTransactionPreventingRevisionReadLock(ignoreTxd) match {
      case Some(txd) => Some(txd)
      case None => if (keyRevisionWriteLocks.isEmpty) None else Some(keyRevisionWriteLocks.head._2)
    }
  }
}
