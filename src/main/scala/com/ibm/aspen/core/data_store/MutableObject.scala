package com.ibm.aspen.core.data_store

import java.util.UUID

import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.objects.{ObjectRefcount, ObjectRevision}
import com.ibm.aspen.core.transaction.TransactionDescription

import scala.concurrent.{Future, Promise}

object MutableObject {
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
abstract class MutableObject(
    val objectId: StoreObjectID, 
    initialOperation: UUID, 
    loader: MutableObjectLoader,
    allocating: Boolean) {
  
  import MutableObject._
  import loader.executionContext

  // Synchronization for the following varialbes is provided by the enclosing DataStoreFrontend instance
  var objectRevisionReadLocks: Map[UUID, TransactionDescription] = Map()
  var objectRevisionWriteLock: Option[TransactionDescription] = None
  var objectRefcountReadLocks: Map[UUID, TransactionDescription] = Map()
  var objectRefcountWriteLock: Option[TransactionDescription] = None
  var deleted: Boolean = false

  // Synchronization for these variables is provided by the MutableObject instance
  protected var meta: ObjectMetadata = ObjectMetadata(NullRevision, NullRefcount, NullTimestamp)
  var pendingOperations: Set[UUID] = Set(initialOperation)
  private[this] var oerr: Option[ObjectReadError] = None
  private[this] var fmeta: Option[Future[Either[ObjectReadError, MutableObject]]] = None
  private[this] var fdata: Option[Future[Either[ObjectReadError, MutableObject]]] = None

  // Used to indicate that the values we have in-memory are authoritative. Once set, reads from
  // disk will not overwrite what we have in memory. The primary case this is used for is to guard
  // against slow reads where the commit happens before the read completes. Here the commit will
  // set one or both of these values to True and the eventual read result will be discarded as
  // obsolete
  private[this] var authoritativeMeta: Boolean = allocating
  private[this] var authoritativeData: Boolean = allocating

  def data: DataBuffer

  /** Called when state is loaded from the store */
  protected def stateLoadedFromBackingStore(m: ObjectMetadata, odb: Option[DataBuffer]): Unit


  def revision: ObjectRevision = synchronized { meta.revision }
  def refcount: ObjectRefcount = synchronized { meta.refcount }
  def timestamp: HLCTimestamp = synchronized { meta.timestamp }

  def metadata: ObjectMetadata = synchronized { meta }

  def setMetadata(m: ObjectMetadata): Unit = synchronized {
    meta = m
  }

  protected def setFullyLoaded(): Unit = synchronized {
    val p = Promise[Either[ObjectReadError, MutableObject]]()
    p.success(Right(this))
    fmeta = Some(p.future)
    fdata = Some(p.future)
  }

  def metadataLoaded: Boolean = synchronized {
    fmeta match {
      case None => false
      case Some(f) => f.isCompleted
    }
  }

  def dataLoaded: Boolean = synchronized {
    fdata match {
      case None => false
      case Some(f) => f.isCompleted
    }
  }

  def bothLoaded: Boolean = synchronized {
    (fmeta, fdata) match {
      case (Some(fm), Some(fd)) => fm.isCompleted && fd.isCompleted
      case _ => false
    }
  }

  def beginOperation(uuid: UUID): Unit = synchronized { pendingOperations += uuid }

  def completeOperation(uuid: UUID): Unit = synchronized {
    pendingOperations -= uuid
    if (pendingOperations.isEmpty)
      loader.unload(this, None)
  }

  def readError: Option[ObjectReadError] = synchronized { oerr }

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

  protected def keepUntilDone(fn: => Future[Unit]): Future[Unit] = {
    val keepUntilCommitComplete = UUID.randomUUID()
    beginOperation(keepUntilCommitComplete)
    val f = fn
    f onComplete(_ => completeOperation(keepUntilCommitComplete))
    f
  }

  def commitMetadata(): Future[Unit] = synchronized {
    authoritativeMeta = true
    val keepUntilCommitComplete = UUID.randomUUID()
    beginOperation(keepUntilCommitComplete)
    val f = loader.backend.putObjectMetaData(objectId, meta)
    f onComplete(_ => completeOperation(keepUntilCommitComplete))
    f
  }

  def commitBoth(): Future[Unit] = synchronized {
    authoritativeMeta = true
    authoritativeData = true
    val keepUntilCommitComplete = UUID.randomUUID()
    beginOperation(keepUntilCommitComplete)
    val f = loader.backend.putObject(objectId, meta, data)
    f onComplete(_ => completeOperation(keepUntilCommitComplete))
    f
  }

  def loadMetadata(): Future[Either[ObjectReadError, MutableObject]] = fmeta match {
    case Some(f) => f
    case None =>

      val fm = loader.backend.getObjectMetaData(objectId) map {
        case Left(err) =>
          synchronized {
            this.oerr = Some(err)
          }
          loader.unload(this, Some(err))
          Left(err)

        case Right(metadata) =>
          synchronized {
            if (!authoritativeMeta) {
              stateLoadedFromBackingStore(metadata, None)
              authoritativeMeta = true
            }
          }
          Right(this)
      }

      fmeta = Some(fm)
      fm
  }

  def loadBoth(): Future[Either[ObjectReadError, MutableObject]] = (fmeta, fdata) match {
    case (Some(fm), Some(fd)) =>
      fm flatMap { _ => fd }

    case _ =>
      val f = loader.backend.getObject(objectId) map {

        case Left(err) =>
          synchronized {
            this.oerr = Some(err)
          }
          loader.unload(this, Some(err))
          Left(err)

        case Right((metadata, data)) =>
          synchronized {
            if (!authoritativeMeta || !authoritativeData) {
              val m = if (authoritativeMeta) this.meta else metadata
              val d = if (authoritativeData) None else Some(data)
              stateLoadedFromBackingStore(m, d)
            }
          }
          Right(this)

      }

      fmeta = Some(f)
      fdata = Some(f)

      f
  }
}