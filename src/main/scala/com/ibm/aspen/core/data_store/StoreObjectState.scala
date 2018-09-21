package com.ibm.aspen.core.data_store

import java.util.UUID

import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.objects.{ObjectRefcount, ObjectRevision}
import com.ibm.aspen.core.transaction.TransactionDescription

import scala.concurrent.{Future, Promise}

object StoreObjectState {
  val NullRevision = ObjectRevision(new UUID(0,0))
  val NullRefcount = ObjectRefcount(0,0)
  val NullTimestamp = HLCTimestamp(0)
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
sealed abstract class StoreObjectState(val frontend: DataStoreFrontend,
                                       val objectId: StoreObjectID,
                                       initialOperation: UUID,
                                       allocationState: Option[(ObjectMetadata, DataBuffer)]) {
  import StoreObjectState._
  import frontend.executionContext

  // Used to track outstanding operations. The set is used instead of a simple reference count to protect
  // against incref/decref coding errors
  var pendingOperations: Set[UUID] = Set(initialOperation)

  var deleted: Boolean = false

  var objectRevisionReadLocks: Map[UUID, TransactionDescription] = Map()
  var objectRevisionWriteLock: Option[TransactionDescription] = None
  var objectRefcountReadLocks: Map[UUID, TransactionDescription] = Map()
  var objectRefcountWriteLock: Option[TransactionDescription] = None

  protected var meta: ObjectMetadata = ObjectMetadata(NullRevision, NullRefcount, NullTimestamp)
  protected var db: DataBuffer = DataBuffer.Empty

  private[this] var fmeta: Option[Future[Either[ObjectReadError, StoreObjectState]]] = None
  private[this] var fdata: Option[Future[Either[ObjectReadError, StoreObjectState]]] = None

  // Used to indicate that the values we have in-memory are authoritative. Once set, reads from
  // disk will not overwrite what we have in memory. The primary case this is used for is to guard
  // against slow reads where the commit happens before the read completes. Here the commit will
  // set one or both of these values to True and the eventual read result will be discarded as
  // obsolete
  private[this] var authoritativeMeta: Boolean = false
  private[this] var authoritativeData: Boolean = false

  // If allocating, we have authoritative state already
  allocationState.foreach { t =>
    val p = Promise[Either[ObjectReadError, StoreObjectState]]()
    p.success(Right(this))

    meta = t._1
    db = t._2
    authoritativeMeta = true
    authoritativeData = false
    fmeta = Some(p.future)
    fdata = Some(p.future)
  }

  protected def dataUpdated(): Unit = {}

  /** Called by frontend when state is loaded from the store */
  def metadataLoaded(m: ObjectMetadata): Unit = if (!authoritativeMeta) {
    authoritativeMeta = true
    meta = m
  }

  /** Called by frontend when state is loaded from the store */
  def dataLoaded(d: DataBuffer): Unit = if (!authoritativeData) {
    authoritativeData = true
    db = d
    dataUpdated()
  }

  def metadata: ObjectMetadata = meta
  def metadata_=(m: ObjectMetadata): Unit = {
    authoritativeMeta = true
    meta = m
  }

  def data: DataBuffer = db
  def data_=(d: DataBuffer): Unit = {
    authoritativeData = true
    db = d
    dataUpdated()
  }

  def revision: ObjectRevision = meta.revision
  def refcount: ObjectRefcount = meta.refcount
  def timestamp: HLCTimestamp = meta.timestamp

  def metadataLoaded: Boolean =  fmeta match {
    case None => false
    case Some(f) => f.isCompleted
  }


  def dataLoaded: Boolean = fdata match {
    case None => false
    case Some(f) => f.isCompleted
  }


  def bothLoaded: Boolean = (fmeta, fdata) match {
    case (Some(fm), Some(fd)) => fm.isCompleted && fd.isCompleted
    case _ => false
  }

  def beginOperation(uuid: UUID): Unit = pendingOperations += uuid

  def completeOperation(uuid: UUID): Unit = {
    pendingOperations -= uuid
    if (pendingOperations.isEmpty)
      frontend.allOperationsCompleted(this)
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

  def commitMetadata(): Future[Unit] = synchronized {
    val keepUntilCommitComplete = UUID.randomUUID()
    beginOperation(keepUntilCommitComplete)
    val f = frontend.backend.putObjectMetaData(objectId, meta)
    f onComplete(_ => completeOperation(keepUntilCommitComplete))
    f
  }

  def commitBoth(): Future[Unit] = synchronized {
    val keepUntilCommitComplete = UUID.randomUUID()
    beginOperation(keepUntilCommitComplete)
    val f = frontend.backend.putObject(objectId, meta, data)
    f onComplete(_ => completeOperation(keepUntilCommitComplete))
    f
  }

  def loadMetadata(): Future[Either[ObjectReadError, StoreObjectState]] = fmeta match {
    case Some(f) => f
    case None =>

      val fm = frontend.backend.getObjectMetaData(objectId) map {
        case Left(err) =>
          frontend.failedToRead(this, err)
          Left(err)

        case Right(metadata) =>
          frontend.loadedObjectState(Some(metadata), None)
          Right(this)
      }

      fmeta = Some(fm)
      fm
  }

  def loadData(): Future[Either[ObjectReadError, StoreObjectState]] = fdata match {
    case Some(f) => f
    case None =>

      val fm = frontend.backend.getObjectData(objectId) map {
        case Left(err) =>
          frontend.failedToRead(this, err)
          Left(err)

        case Right(data) =>
          frontend.loadedObjectState(None, Some(data))
          Right(this)
      }

      fmeta = Some(fm)
      fm
  }

  def loadBoth(): Future[Either[ObjectReadError, StoreObjectState]] = (fmeta, fdata) match {
    case (Some(fm), Some(fd)) =>
      fm flatMap { _ => fd }

    case _ =>
      val f = frontend.backend.getObject(objectId) map {

        case Left(err) =>
          frontend.failedToRead(this, err)
          Left(err)

        case Right((metadata, data)) =>
          frontend.loadedObjectState(Some(metadata), Some(data))
          Right(this)

      }

      fmeta = Some(f)
      fdata = Some(f)

      f
  }
}
