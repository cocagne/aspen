package com.ibm.aspen.core.data_store

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.DataBuffer
import java.nio.ByteBuffer
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.objects.keyvalue.Key
import scala.concurrent.Future
import scala.concurrent.Promise

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
    loader: MutableObjectLoader) {
  
  import MutableObject._
  
  import loader.executionContext
  
  var revision: ObjectRevision = NullRevision
  var refcount: ObjectRefcount = NullRefcount
  var timestamp: HLCTimestamp = NullTimestamp 
  protected var dataBuffer: DataBuffer = DataBuffer.Empty
  var objectRevisionReadLocks: Map[UUID, TransactionDescription] = Map()
  var objectRevisionWriteLock: Option[TransactionDescription] = None
  var objectRefcountReadLocks: Map[UUID, TransactionDescription] = Map()
  var objectRefcountWriteLock: Option[TransactionDescription] = None
  var pendingOperations: Set[UUID] = Set(initialOperation)
  
  private[this] var err: Option[ObjectReadError] = None
  private[this] var fmeta: Option[Future[Either[ObjectReadError, MutableObject]]] = None
  private[this] var fdata: Option[Future[Either[ObjectReadError, MutableObject]]] = None
  
  def data: DataBuffer = dataBuffer
  
  def metadata = ObjectMetadata(revision, refcount, timestamp)
  
  protected def setState(meta: ObjectMetadata, content: DataBuffer): Unit = {
    metadata = meta
    dataBuffer = content
    val p = Promise[Either[ObjectReadError, MutableObject]]()
    p.success(Right(this))
    fmeta = Some(p.future)
    fdata = Some(p.future)
  }
  
  def metadata_=(m: ObjectMetadata) {
    revision = m.revision
    refcount = m.refcount
    timestamp = m.timestamp
  }
  
  def metadataLoaded: Boolean = fmeta match {
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
      loader.unload(this, None)
  }
  
  def readError: Option[ObjectReadError] = err
  
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
  
  def keepUntilDone(fn: => Future[Unit]): Future[Unit] = {
    val keepUntilCommitComplete = UUID.randomUUID()
    beginOperation(keepUntilCommitComplete)
    val f = fn
    f onComplete {
      case _ => completeOperation(keepUntilCommitComplete)
    }
    f
  }
  
  def commitMetadata(): Future[Unit] = keepUntilDone(loader.backend.putObjectMetaData(objectId, ObjectMetadata(revision, refcount, timestamp)))
  
  def commitData(): Future[Unit] = keepUntilDone(loader.backend.putObjectData(objectId, data))
  
  def commitBoth(): Future[Unit] = keepUntilDone(loader.backend.putObject(objectId, ObjectMetadata(revision, refcount, timestamp), data))
  
  def loadMetadata(): Future[Either[ObjectReadError, MutableObject]] = fmeta match {
    case Some(f) => f
    case None =>
      val fm = loader.backend.getObjectMetaData(objectId) map { e => e match {
        case Left(err) => 
          this.err = Some(err)
          loader.unload(this, Some(err))
          Left(err)
          
        case Right(metadata) =>
          this.metadata = metadata
          Right(this)
      }}
      fmeta = Some(fm)
      fm
  }
  
  def loadData(): Future[Either[ObjectReadError, MutableObject]] = fdata match {
    case Some(f) => f
    case None => 
      val fd = loader.backend.getObjectData(objectId) map { e => e match {
        case Left(err) => 
          this.err = Some(err)
          loader.unload(this, Some(err))
          Left(err)
          
        case Right(data) =>
          this.dataBuffer = data
          Right(this)
      }}
      fdata = Some(fd)
      fd
  }
  
  def loadBoth(): Future[Either[ObjectReadError, MutableObject]] = (fmeta, fdata) match {
    case (None, None) =>
      val f = loader.backend.getObject(objectId) map { e => e match {
        case Left(err) => 
          this.err = Some(err)
          loader.unload(this, Some(err))
          Left(err)
          
        case Right((metadata, data)) =>
          this.metadata = metadata
          this.dataBuffer = data
          Right(this)
      }}
      fmeta = Some(f)
      fdata = Some(f)
      f
      
    case (Some(fm), None) => 
      val fd = loadData()
      fm flatMap { _ => fd }
      
    case (None, Some(fd)) =>
      val fm = loadMetadata()
      fd flatMap { _ => fm }
      
    case (Some(fm), Some(fd)) => 
      fm flatMap { _ => fd }
  }
}