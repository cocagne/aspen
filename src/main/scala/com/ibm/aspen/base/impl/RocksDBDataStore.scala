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
import com.ibm.aspen.core.data_store.InvalidLocalPointer
import com.ibm.aspen.core.data_store.RevisionMismatch
import com.ibm.aspen.core.data_store.RefcountMismatch
import com.ibm.aspen.core.data_store.ObjectReadError
import scala.concurrent.Promise
import com.ibm.aspen.core.data_store.ObjectTransactionError
import com.ibm.aspen.core.data_store.ObjectTransactionError
import com.ibm.aspen.core.data_store.TransactionReadError
import com.ibm.aspen.core.data_store.TransactionCollision
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.transaction.RefcountUpdate
import com.ibm.aspen.core.transaction.DataUpdate
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import com.ibm.aspen.core.allocation.Allocate

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
  
  /** Represents the current state of the object. Changes are made here first and then written to disk.
   *  Instances of this class will be maintained in-memory as long as the pendingOperations set remains
   *  non-empty. So long as there are one or more outstanding operations that will need to reference the
   *  working state in the future, it's preferable from a performance and synchronization perspective to
   *  keep it in memory rather than re-loading from the database.
   */
  private class WorkingState(
      val objectUUID: UUID,
      var revision:ObjectRevision, 
      var refcount: ObjectRefcount, 
      var lastTxUUID: UUID,
      var data: DataBuffer, 
      var lockedTransaction: Option[TransactionDescription],
      var pendingOperations: Set[UUID])
      
  /** Tracks all operations attempting to load the same WorkingState */
  private class LoadingState(initialOperation: UUID) {
    val loadPromise = Promise[Either[ObjectReadError, WorkingState]]()
    
    var pendingOperations = Set[UUID](initialOperation)
    
    def addOperation(opUUID: UUID) = pendingOperations += opUUID
  }
  
  class Factory(dbPath:String)(implicit ec: ExecutionContext) extends DataStore.Factory {
    def apply(
        storeId: DataStoreID,
        transactionRecoveryStates: List[TransactionRecoveryState],
        allocationRecoveryStates: List[AllocationRecoveryState]): Future[DataStore] = {
      val ds = new RocksDBDataStore(storeId, dbPath, transactionRecoveryStates, allocationRecoveryStates)(ec)
      ds.initialized
    }
  }
}

class RocksDBDataStore(
    val storeId: DataStoreID,
    dbPath:String,
    transactionRecoveryStates: List[TransactionRecoveryState], 
    allocationRecoveryStates: List[AllocationRecoveryState])(implicit ec: ExecutionContext) extends DataStore with BootstrapDataStore {
  
  import RocksDBDataStore._
  
  private[this] val db = new BufferedConsistentRocksDB(dbPath)
  
  /** Holds map of objectUUID -> loadingState. Each operation requiring the WorkingState is identified by a UUID. For transaction-based operations, the UUID
   *  will be that of the transaction. For simple reads, it'll be a randomly-generated UUID. The goal of the loading state is to track all operations
   *  requesting the same WorkingState while the initial load is in progress. Once the load is complete, the set will be transfered to to the
   *  WorkingState object as its set of pendingOperations.
   */
  private[this] var loadingStates = Map[UUID, LoadingState]() 
  
  /** Holds the current object state while transactions are outstanding and/or data has yet to be committed to disk. */
  private[this] var workingStates = Map[UUID, WorkingState]()
  
  private[this] var allocations = Map[UUID, AllocationRecoveryState]()
  
  private[this] def isHostedObject(op: ObjectPointer) = op.poolUUID == storeId.poolUUID && op.storePointers.find(_.poolIndex == storeId.poolIndex).isDefined
  
  private[this] def getHostedObjects(txd:TransactionDescription) = txd.allReferencedObjectsSet.filter(isHostedObject)
  
  def close() = db.close()
  
  val initialized: Future[DataStore] = synchronized {
    var flocks = List[Future[Unit]]()
    
    getTransactionsToBeLocked(transactionRecoveryStates).foreach { trs => getHostedObjects(trs.txd).foreach(op => {
      flocks = getObject(op).map(r => r match {
        case Left(err) => // ??? Shouldn't be possible (except may be for corruption) and there's nothing we can do about it. Log & continue on?
        case Right((state, data)) => 
          workingStates += (op.uuid -> new WorkingState(op.uuid, state.revision, state.refcount, state.lastCommittedTxUUID, data, Some(trs.txd), Set(trs.txd.transactionUUID)))
      }) :: flocks
    })
    }

    Future.sequence(flocks).map(_=> this)
  }
  
  def bootstrapAllocateNewObject(objectUUID: UUID, initialContent: DataBuffer): Future[StorePointer] = synchronized {
      val initialRevision = ObjectRevision(0, initialContent.size)
      val initialRefcount = ObjectRefcount(0, 1)
      val buf = initialContent.getByteArray()
      db.put(stateKey(objectUUID), stateToBytes(initialRevision, initialRefcount, Bootstrap.BootstrapTransactionUUID))
      db.put(dataKey(objectUUID), buf).map(_ => StorePointer(storeId.poolIndex, new Array[Byte](0)))
  }
  
  def bootstrapOverwriteObject(objectPointer: ObjectPointer, newContent: DataBuffer): Future[Unit] = synchronized {
    val initialRevision = ObjectRevision(0, newContent.size)
    val initialRefcount = ObjectRefcount(0, 1)
    val buf = newContent.getByteArray()
    db.put(stateKey(objectPointer), stateToBytes(initialRevision, initialRefcount, Bootstrap.BootstrapTransactionUUID))
    db.put(dataKey(objectPointer), buf).map(_ => ())
  }
  
  private[this] def completeWorkingStateOperation(objectUUID: UUID, ws: WorkingState, operationUUID: UUID): Unit = synchronized {
    ws.pendingOperations -= operationUUID
    if (ws.pendingOperations.isEmpty)
      workingStates -= objectUUID
  }
  
  /** Allocates a new Object on the store */
  def allocate(newObjects: List[Allocate.NewObject],
               allocationTransactionUUID: UUID,
               allocatingObject: ObjectPointer,
               allocatingObjectRevision: ObjectRevision): Future[Either[AllocationErrors.Value, AllocationRecoveryState]] = synchronized {
                 
    val lst = newObjects map { no =>      
      AllocationRecoveryState.NewObject(
          StorePointer(storeId.poolIndex, new Array[Byte](0)), 
          no.newObjectUUID, no.objectSize, no.objectData, no.initialRefcount)
    }
    
    val ars = AllocationRecoveryState(storeId, lst, allocationTransactionUUID, allocatingObject, allocatingObjectRevision)
    
    synchronized {
      
      ars.newObjects.foreach { newObj =>
        
        val ws = new WorkingState(newObj.newObjectUUID, ObjectRevision(0, newObj.objectData.size), newObj.initialRefcount, 
                                  ars.allocationTransactionUUID, newObj.objectData, None, Set(ars.allocationTransactionUUID))
        
        allocations += (newObj.newObjectUUID -> ars)
        workingStates += (newObj.newObjectUUID -> ws)
      }
    }
    
    Future.successful(Right(ars))
  }
  
  def allocationResolved(ars: AllocationRecoveryState, committed: Boolean): Future[Unit] = synchronized {
    var flist = List[Future[Unit]]()
    
    if (committed) {
      ars.newObjects.foreach { newObj =>
        // Reads of mid-allocation objects force a commit to ensure that transactions against those objects
        // wont be accidentally overwritten by the initial state if the initial write to disk is slow for some
        // reason. That causes this method to be called twice.
        if (allocations.contains(newObj.newObjectUUID)) {
          
          allocations -= newObj.newObjectUUID
          
          workingStates.get(newObj.newObjectUUID).foreach { ws =>
    
            db.put(stateKey(newObj.newObjectUUID), stateToBytes(ws.revision, ws.refcount, ars.allocationTransactionUUID))
            
            flist = db.put(dataKey(newObj.newObjectUUID), newObj.objectData.getByteArray()) :: flist 
            
            flist.head onComplete {
              case _ => completeWorkingStateOperation(newObj.newObjectUUID, ws, ars.allocationTransactionUUID)
            }
          }
        }
      }
    } else {
      List(Future.successful(()))
    }
    
    Future.sequence(flist) map (_=>())
  }
  
  def getObject(objectPointer: ObjectPointer, storePointer: StorePointer): Future[Either[ObjectReadError, (CurrentObjectState,DataBuffer)]] = {
    getWorkingState(objectPointer, None) map { e => e match {
      case Left(err) => Left(err)
      case Right(ws) => Right((CurrentObjectState(objectPointer.uuid, ws.revision, ws.refcount, ws.lastTxUUID, ws.lockedTransaction), ws.data))
    }}
  }
  
  private def getWorkingState(objectPointer: ObjectPointer, transactionUUID: Option[UUID]): Future[Either[ObjectReadError, WorkingState]] = synchronized {
    workingStates.get(objectPointer.uuid) match {
      case Some(ws) => 
        transactionUUID.foreach( txuuid => ws.pendingOperations += txuuid )
        
        allocations.get(objectPointer.uuid) match { 
          case Some(ars) =>
            // Objects cannot be read until after they are successfully allocated. We must be slow in realizing this so force the write to occur now
            // to ensure we cannot have some other transaction successfully commit before the initial state is written to disk.
            val fsave = allocationResolved(ars, true)
            fsave.map(_ => Right(ws))
            
          case None =>
            Future.successful(Right(ws))
        }
        
      case None =>
        val operationUUID = transactionUUID.getOrElse(UUID.randomUUID())
        val fws = loadWorkingState(objectPointer, operationUUID)
        fws map { e => e match {
          case Left(err) => Left(err)
          case Right(ws) =>
            if (!transactionUUID.isDefined)
              completeWorkingStateOperation(objectPointer.uuid, ws, operationUUID)
            Right(ws)        
        }} 
    }
  }
  
  // To be called only by getWorkingState
  private[this] def loadWorkingState(objectPointer: ObjectPointer, operationUUID: UUID): Future[Either[ObjectReadError, WorkingState]] = synchronized {

    val loadingState = new LoadingState(operationUUID)
    
    loadingStates += (objectPointer.uuid -> loadingState)
    
    val fstate = db.get(stateKey(objectPointer.uuid))
    val fdata = db.get(dataKey(objectPointer.uuid))
    
    for {
      ostate <- fstate
      odata <- fdata
    } yield {
      (ostate, odata) match {
        case (Some(stateBuf), Some(dataBuf)) =>
          val (revision, refcount, lastTxUUID) = bytesToState(stateBuf)
          synchronized {
            val ws = new WorkingState(objectPointer.uuid, revision, refcount, lastTxUUID, DataBuffer(dataBuf), None, loadingState.pendingOperations)
            loadingStates -= objectPointer.uuid
            workingStates += (objectPointer.uuid -> ws)
            loadingState.loadPromise.success(Right(ws))
          }
        case _ => synchronized {
          loadingStates -= objectPointer.uuid
          loadingState.loadPromise.success(Left(new InvalidLocalPointer))
        }
      }
    }
    
    loadingState.loadPromise.future
  }
  
  /** Attempts to locks all objects referenced by the transaction that are hosted by this store.
   *  
   *  If the returned list of errors is empty, the transaction successfully locked all objects. If any errors are returned,
   *  no object locks are granted.
   */
  def lockTransaction(txd: TransactionDescription, updateData: Option[List[LocalUpdate]]): Future[List[ObjectTransactionError]] = {
    
    val checker = new TransactionErrorChecker(txd, updateData)
    
    val floadWs = Future.sequence(getHostedObjects(txd).map(op => getWorkingState(op, Some(txd.transactionUUID)).map(e => (op,e))))
    
    floadWs map { rset =>
 
      val omap = rset.foldLeft(Map[UUID, Either[ObjectReadError, WorkingState]]()) { (m, t) => 
        t._2 match {
            case Left(err) => m + (t._1.uuid -> Left(err))
            case Right(ws) => m + (t._1.uuid -> Right(ws))
        }
      }
      
      synchronized {
      
        def getCurrentState(op: ObjectPointer, sp:StorePointer): Either[ObjectReadError, (ObjectRevision, ObjectRefcount, Option[TransactionDescription])] = {
          omap.get(op.uuid) match {
            case None => Left(new InvalidLocalPointer)
            case Some(e) => e match {
              case Left(err) => Left(err)
              case Right(ws) => Right((ws.revision, ws.refcount, ws.lockedTransaction))
            }
          }
        }
        
        val errors = checker.getErrors(getCurrentState)
        
        if (errors.isEmpty)
          omap.foreach(t => t._2 match {
            case Left(_) =>
            case Right(ws) => ws.lockedTransaction = Some(txd)
          })
        
        errors
      }
    }
  }
  
  /** Commits the transaction changes and returns a Future to the completion of the commit operation.
   *  
   *  This method always returns Success() since there are no recovery steps the transaction logic can take for failures
   *  that occur after the commit decision has been made. 
   */
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[List[LocalUpdate]]): Future[Unit] = synchronized {
    
    var dataUpdates = Set[WorkingState]()
    
    val floads = getHostedObjects(txd).map{ op => 
      getWorkingState(op, Some(txd.transactionUUID)).map(e => (op.uuid -> e))
    }
    
    Future.sequence(floads).flatMap { allObjects => synchronized {
      
      // Filter down to just the successfully loaded objects. 
      val loadedObjects = allObjects.foldLeft(Map[UUID, WorkingState]()) { (m, t) => t._2 match {
        case Left(err) => m // Nothing we can do :(
        case Right(ws) => m + (t._1 -> ws)
      }}
      
      val objectUpdates = localUpdates match {
        case None => Map[UUID, DataBuffer]()
        case Some(lst) => lst.map(lu => (lu.objectUUID -> lu.data)).toMap
      }
      
      txd.requirements.foreach { r =>
        loadedObjects.get(r.objectPointer.uuid).foreach { ws => r match {
          case ru: RefcountUpdate => ws.refcount = ru.newRefcount
            
          case du: DataUpdate =>
            objectUpdates.get(r.objectPointer.uuid).foreach { data =>
              dataUpdates += ws
              
              du.operation match {
                case DataUpdateOperation.Overwrite => 
                ws.data = data
                ws.revision = ws.revision.overwrite(data.size)
                
              case DataUpdateOperation.Append => 
                if (ws.revision == du.requiredRevision) {
                  val buf = ByteBuffer.allocate( ws.data.size + data.size )
                  buf.put(ws.data.asReadOnlyBuffer())
                  buf.put(data.asReadOnlyBuffer())
                  buf.position(0)
                  ws.data = DataBuffer(buf)
                  ws.revision = ws.revision.append(data.size)
                }
              }
            }
          }
        }
      }
      
      var commits = List[Future[Unit]]()
      
      loadedObjects.foreach(t => {
          commits = db.put(stateKey(t._1), stateToBytes(t._2.revision, t._2.refcount, txd.transactionUUID)) :: commits
          
          if (dataUpdates.contains(t._2))   
            db.put(dataKey(t._1), t._2.data.getByteArray())
          
          t._2.lockedTransaction = None
          
          completeWorkingStateOperation(t._2.objectUUID, t._2, txd.transactionUUID)
          
        })
      
     
      Future.sequence(commits).map(_ => ())
    }}
  }
  
  
  /** Called at the end of each transaction to ensure all object locks are released.
   *  
   *  For successful transactions, commitTransactionUpdates will be called first and it should release the
   *  locks while the finalization actions run. Both committed and aborted transactions call this method.
   * 
   */
  def discardTransaction(txd: TransactionDescription): Unit = synchronized { 
    getHostedObjects(txd).foreach(op => workingStates.get(op.uuid).foreach(ws => {
      
      completeWorkingStateOperation(op.uuid, ws, txd.transactionUUID)
      
      ws.lockedTransaction.foreach( lockedTxd => if (txd == lockedTxd) ws.lockedTransaction = None )
      
    }))
  }
}