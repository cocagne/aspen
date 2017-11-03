package com.ibm.aspen.base.impl

import com.ibm.aspen.core.data_store.DataStore
import java.util.UUID
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.concurrent.Future
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.allocation.AllocationErrors
import java.nio.ByteBuffer
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.data_store.ObjectError
import com.ibm.aspen.core.data_store.CurrentObjectState
import scala.concurrent.ExecutionContext
import com.ibm.aspen.core.transaction.DataUpdateOperation
import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.data_store.BootstrapDataStore

object RocksDBDataStore {
  val StateIndex:Byte = 0
  val DataIndex:Byte = 1
  
  private [this] def tokey(objectUUID:UUID, index:Byte) = {
    val bb = ByteBuffer.allocate(17)
    bb.putLong(0, objectUUID.getMostSignificantBits)
    bb.putLong(8, objectUUID.getLeastSignificantBits)
    bb.put(16, index)
    bb.array()
  }
  
  private def stateKey(objectUUID:UUID) = tokey(objectUUID, StateIndex)
  private def dataKey(objectUUID:UUID) = tokey(objectUUID, DataIndex)
  
  private def stateKey(objectPointer:ObjectPointer) = tokey(objectPointer.uuid, StateIndex)
  private def dataKey(objectPointer:ObjectPointer) = tokey(objectPointer.uuid, DataIndex)
  
  private def stateToBytes(rev: ObjectRevision, ref: ObjectRefcount, lastTransactionUUID: UUID): Array[Byte] = {
    val bb = ByteBuffer.allocate(32)
    bb.putInt(0, rev.overwriteCount)
    bb.putInt(4, rev.currentSize)
    bb.putInt(8, ref.updateSerial)
    bb.putInt(12, ref.count)
    bb.putLong(16, lastTransactionUUID.getMostSignificantBits)
    bb.putLong(24, lastTransactionUUID.getLeastSignificantBits)
    bb.array()
  }
  private def bytesToState(buf:Array[Byte]): (ObjectRevision, ObjectRefcount, UUID) = {
    val bb = ByteBuffer.wrap(buf)
    val rev = ObjectRevision(bb.getInt(0), bb.getInt(4))
    val ref = ObjectRefcount(bb.getInt(8), bb.getInt(12)) 
    val txuuid = new UUID(bb.getLong(16), bb.getLong(24))
    (rev, ref, txuuid)
  }
  
  def bytebufToArray(buf: ByteBuffer): Array[Byte] = {
    val a = new Array[Byte](buf.limit - buf.position)
    buf.asReadOnlyBuffer().get(a)
    a
  }
  
  private class WorkingState(
      var revision:ObjectRevision, 
      var refcount: ObjectRefcount, 
      var lastTxUUID: UUID,
      var data: ByteBuffer, 
      var lockedTransaction: Option[TransactionDescription],
      var pendingTransactions: Set[TransactionDescription])
}

class RocksDBDataStore(
    val storeId: DataStoreID,
    dbPath:String)(implicit ec: ExecutionContext) extends DataStore with BootstrapDataStore {
  
  import RocksDBDataStore._
  
  private [this] val db = new BufferedConsistentRocksDB(dbPath)
  
  /** Holds the current object state while transactions are outstanding and/or data has yet to be committed to disk */
  private [this] var workingState = Map[UUID, WorkingState]()
  
  private [this] def isHostedObject(op: ObjectPointer) = op.poolUUID == storeId.poolUUID && op.storePointers.find(_.poolIndex == storeId.poolIndex).isDefined
  
  private [this] def getHostedObjects(txd:TransactionDescription) = txd.allReferencedObjectsSet.filter(isHostedObject)
  
  def close() = db.close()
  
  def initialize(transactionRecoveryStates: List[TransactionRecoveryState]): Future[Unit] = synchronized {
    var flocks = List[Future[Unit]]()
    
    getTransactionsToBeLocked(transactionRecoveryStates).foreach(trs => getHostedObjects(trs.txd).foreach(op => {
      flocks = getObject(op).map(r => r match {
        case Left(err) => // ??? Shouldn't be possible and there's nothing we can do about it. Log?
        case Right((state, data)) => 
          workingState += (op.uuid -> new WorkingState(state.revision, state.refcount, state.lastCommittedTxUUID, data, Some(trs.txd), Set(trs.txd)))
      }) :: flocks
    })
    )
    
    Future.sequence(flocks).map(_=>())
  }
  
  def bootstrapAllocateNewObject(objectUUID: UUID, initialContent: ByteBuffer): Future[StorePointer] = synchronized {
      val initialRevision = ObjectRevision(0, initialContent.limit - initialContent.position)
      val initialRefcount = ObjectRefcount(0, 1)
      val buf = bytebufToArray(initialContent)
      db.put(stateKey(objectUUID), stateToBytes(initialRevision, initialRefcount, Bootstrap.BootstrapTransactionUUID))
      db.put(dataKey(objectUUID), buf).map(_ => StorePointer(storeId.poolIndex, new Array[Byte](0)))
  }
  
  def bootstrapOverwriteObject(objectPointer: ObjectPointer, newContent: ByteBuffer): Future[Unit] = synchronized {
    val initialRevision = ObjectRevision(0, newContent.limit - newContent.position)
    val initialRefcount = ObjectRefcount(0, 1)
    val buf = bytebufToArray(newContent)
    db.put(stateKey(objectPointer), stateToBytes(initialRevision, initialRefcount, Bootstrap.BootstrapTransactionUUID))
    db.put(dataKey(objectPointer), buf).map(_ => ())
  }
  
  /** TODO Failure handling. Currently we never check to see if the allocation succeeded/failed
   */
  def allocateNewObject(objectUUID: UUID, 
                        size: Option[Int], 
                        initialContent: ByteBuffer,
                        initialRefcount: ObjectRefcount,
                        allocationTransactionUUID: UUID,
                        allocatingObject: ObjectPointer,
                        allocatingObjectRevision: ObjectRevision): Future[Either[AllocationErrors.Value, StorePointer]] = {
    val f = synchronized {
      val initialRevision = ObjectRevision(0, initialContent.limit - initialContent.position)
      db.put(stateKey(objectUUID), stateToBytes(initialRevision, initialRefcount, allocationTransactionUUID))
      val buf = bytebufToArray(initialContent)
      val fcommitted = db.put(dataKey(objectUUID), buf)
      
      val ws = new WorkingState(initialRevision, initialRefcount, allocationTransactionUUID, initialContent, None, Set())
      
      workingState += (objectUUID -> ws)
      
      fcommitted onComplete {
        case _ => synchronized {
          if (ws.pendingTransactions.isEmpty)
            workingState -= objectUUID
        }
      }
      
      fcommitted
    }
    
    f.map(_ => Right(StorePointer(storeId.poolIndex, new Array[Byte](0))))
  }
  
  def getObject(objectPointer: ObjectPointer, storePointer: StorePointer): Future[Either[ObjectError.Value, (CurrentObjectState,ByteBuffer)]] = {
    synchronized {
      workingState.get(objectPointer.uuid)} match {
      case Some(ws) => Future.successful(Right((CurrentObjectState(objectPointer.uuid, ws.revision, ws.refcount, ws.lastTxUUID, ws.lockedTransaction), ws.data)))
      case None =>
        val fstate = db.get(stateKey(objectPointer.uuid))
        val fdata = db.get(dataKey(objectPointer.uuid))
        for {
          ostate <- fstate
          odata <- fdata
        } yield {
          (ostate, odata) match {
            case (Some(stateBuf), Some(dataBuf)) =>
              val (revision, refcount, lastTxUUID) = bytesToState(stateBuf)
              Right((CurrentObjectState(objectPointer.uuid, revision, refcount, lastTxUUID, None), ByteBuffer.wrap(dataBuf)))
            case _ => Left(ObjectError.InvalidLocalPointer)
          }
        }
    }
  }
  
  /* This method must be called at the beginning of a transaction before any decisions can be made. To allow lockOrCollide to complete
   * immediately and without preforming any I/O, we'll load all local objects into the workingState map and keep them there until the
   * transaction commits/aborts.
   */
  def getCurrentObjectState(txd: TransactionDescription): Future[Map[UUID, Either[ObjectError.Value, CurrentObjectState]]] = {
    val localObjects = getHostedObjects(txd)
    
    val futureSet = localObjects.map(op => getObject(op).map(r => r match {
      case Left(err) => (op.uuid -> Left(err))
      case Right((state, data)) =>
        synchronized {
          workingState.get(op.uuid) match {
            case Some(ws) => ws.pendingTransactions += txd
            case None => workingState += (op.uuid -> new WorkingState(state.revision, state.refcount, state.lastCommittedTxUUID, data, None, Set(txd)))
          }
        }
        (op.uuid -> Right(state))
    }))
    
    Future.sequence(futureSet).map( _.toMap )
  }
  
  
  /** Locks all objects referenced by the transaction or returns a map of collisions and/or errors. Note that this method
   *  must detect Revision and Refcount mismatch errors. getCurrentObjectState is used by transactions to do initial error
   *  checking on the refcount and revision but it is possible for those values to change between that call and this call.
   */
  def lockOrCollide(txd: TransactionDescription): Option[Map[UUID, Either[ObjectError.Value, TransactionDescription]]] = synchronized {
    // Note that objects can be deleted between getCurrentObjectState and the call here. If they're not in the working set, provide an error.
    // Also, we can only report one error per object so subsequent checks will overwrite previously found errors
    //
    var localObjects = Map[UUID, WorkingState]()
    var errs =  Map[UUID, Either[ObjectError.Value, TransactionDescription]]()
    
    // Check for object existence and locked transactions
    for (op <- getHostedObjects(txd)) {
      workingState.get(op.uuid) match {
        case None => errs += (op.uuid -> Left(ObjectError.InvalidLocalPointer))
        case Some(ws) =>
          localObjects += (op.uuid -> ws)
          ws.lockedTransaction.foreach( lockedTxd => errs += (op.uuid -> Right(lockedTxd)) )
      }
    }
    
    // Check Refcounts
    txd.refcountUpdates.foreach(ru => localObjects.get(ru.objectPointer.uuid).foreach(ws =>{
      if (ws.refcount != ru.requiredRefcount)
        errs += (ru.objectPointer.uuid -> Left(ObjectError.RefcountMismatch))
    }))
    
    // Check Revisions
    txd.dataUpdates.foreach(du => localObjects.get(du.objectPointer.uuid).foreach(ws =>{
      if (ws.revision != du.requiredRevision)
        errs += (du.objectPointer.uuid -> Left(ObjectError.RevisionMismatch))
    }))
    
    if (errs.isEmpty) {
      localObjects.foreach(t => t._2.lockedTransaction = Some(txd))
      None
    } else
      Some(errs)
  }
  
  
  /** Commits the transaction changes and returns a Future to the completion of the commit operation.
   *  
   *  This method always returns Success() since there are no recovery steps the transaction logic can take for failures
   *  that occur after the commit decision has been made. 
   */
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[Array[ByteBuffer]]): Future[Unit] = synchronized {
    
    var localObjects = Map[UUID, WorkingState]()
    var dataUpdates = Set[WorkingState]()
    
    getHostedObjects(txd).foreach(op => workingState.get(op.uuid).foreach(ws => localObjects += (op.uuid -> ws)))
    
    txd.refcountUpdates.foreach(ru => localObjects.get(ru.objectPointer.uuid).foreach(ws => ws.refcount = ru.newRefcount)) 
    
    // Update object data only if we have the data to do so
    localUpdates.foreach( updateData => if (updateData.size == txd.dataUpdates.size) {
      txd.dataUpdates.zipWithIndex.foreach(t => localObjects.get(t._1.objectPointer.uuid).foreach(ws =>{
        dataUpdates += ws
        t._1.operation match {
          case DataUpdateOperation.Overwrite => 
            ws.data = updateData(t._2)
            ws.revision = ObjectRevision(ws.revision.overwriteCount + 1, ws.data.capacity)
          case DataUpdateOperation.Append =>
            val app = updateData(t._2)
            val buf = ByteBuffer.allocate( ws.data.capacity + app.capacity )
            buf.put(ws.data)
            buf.put(app)
            buf.position(0)
            ws.data = buf
            ws.revision = ObjectRevision(ws.revision.overwriteCount, ws.data.capacity)
        }
      }))
    })
    
    var commits = List[Future[Unit]]()
    
    localObjects.foreach(t => {
      commits = db.put(stateKey(t._1), stateToBytes(t._2.revision, t._2.refcount, txd.transactionUUID)) :: commits
      
      if (dataUpdates.contains(t._2)) {
        val data = if (t._2.data.isDirect) {
          val buf = new Array[Byte](t._2.data.capacity)
          t._2.data.asReadOnlyBuffer().get(buf)
          buf
        } else
          t._2.data.array
        db.put(dataKey(t._1), data)
      }
      
      t._2.lockedTransaction = None
      t._2.pendingTransactions -= txd
      
      if (t._2.pendingTransactions.isEmpty)
        workingState -= t._1
    })
   
    Future.sequence(commits).map(_ => ())
  }
  
  
  /** Called at the end of each transaction to ensure all object locks are released.
   *  
   *  For successful transactions, commitTransactionUpdates will be called first and it should release the
   *  locks while the finalization actions run. Both committed and aborted transactions call this method.
   * 
   */
  def discardTransaction(txd: TransactionDescription): Unit = synchronized { 
    getHostedObjects(txd).foreach(op => workingState.get(op.uuid).foreach(ws => {
      ws.pendingTransactions -= txd
      
      ws.lockedTransaction.foreach( lockedTxd => if (txd == lockedTxd) ws.lockedTransaction = None )
      
      if (ws.pendingTransactions.isEmpty)
        workingState -= op.uuid
    }))
  }
}