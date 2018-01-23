package com.ibm.aspen.core.data_store

import com.ibm.aspen.core.transaction.TransactionRecoveryState
import com.ibm.aspen.core.allocation.AllocationRecoveryState
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import java.util.UUID
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.transaction.DataUpdate
import com.ibm.aspen.core.transaction.RefcountUpdate
import com.ibm.aspen.core.transaction.VersionBump
import com.ibm.aspen.core.transaction.LocalUpdate
import scala.concurrent.Promise
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.ObjectPointer
import scala.util.Success
import com.ibm.aspen.core.objects.StorePointer
import com.ibm.aspen.core.allocation.Allocate
import com.ibm.aspen.core.allocation.AllocationErrors
import com.ibm.aspen.core.transaction.DataUpdateOperation
import com.ibm.aspen.core.transaction.TransactionDisposition
import com.ibm.aspen.core.allocation.DataAllocationOptions
import com.ibm.aspen.core.objects.ObjectType
import com.ibm.aspen.core.allocation.KeyValueAllocationOptions


class DataStoreFrontend(
    val storeId: DataStoreID,
    val backend: DataStoreBackend,
    transactionRecoveryStates: List[TransactionRecoveryState], 
    allocationRecoveryStates: List[AllocationRecoveryState]
    ) extends DataStore { 
  
  override implicit val executionContext: ExecutionContext = backend.executionContext
  
  private[this] var objectLoader = new MutableObjectLoader(backend)

  private[this] var lockedTransactions = Map[UUID, StoreTransaction]()
  
  override val initialized: Future[DataStore] = synchronized {
    
    allocationRecoveryStates.foreach(loadAllocatedObjects)
    
    // Re-establish locks on all transactions we voted to commit
    
    val txToLock = transactionRecoveryStates.filter(trs => trs.disposition == TransactionDisposition.VoteCommit) 
    
    val flocks = txToLock.foldLeft(List[Future[Unit]]()) { (l, trs) => 
      val t = new StoreTransaction(trs.txd, trs.localUpdates.getOrElse(Nil))
      
      t.lockObjects()
      
      lockedTransactions += (trs.txd.transactionUUID -> t)
      
      t.objectsLoaded :: l
    }

    Future.sequence(flocks).map(_=> this)
  }
  
  override def close(): Future[Unit] = synchronized {
    backend.close()
  }
  
  override def bootstrapAllocateNewObject(objectUUID: UUID, initialContent: DataBuffer, timestamp: HLCTimestamp): Future[StorePointer] = synchronized {
    val metadata = ObjectMetadata(ObjectRevision(new UUID(0,0)), ObjectRefcount(0,1), timestamp)
    
    backend.allocateObject(objectUUID, metadata, initialContent) flatMap { e => e match { 
      case Left(err) => throw new Exception(s"Allocation failed: $err")
      case Right(arr) => 
        val sp = new StorePointer(storeId.poolIndex, arr)
        backend.putObject(StoreObjectID(objectUUID, sp), metadata, initialContent).map( _ => sp )
    }}
  }
  
  override def bootstrapOverwriteObject(objectPointer: ObjectPointer, newContent: DataBuffer, timestamp: HLCTimestamp): Future[Unit] = synchronized {
    val objectId = StoreObjectID(objectPointer.uuid, objectPointer.getStorePointer(storeId).get)
    val metadata = ObjectMetadata(ObjectRevision(new UUID(0,0)), ObjectRefcount(0,1), timestamp)
    
    backend.putObject(objectId, metadata, newContent)
  }
  
  override def allocate(
      newObjects: List[Allocate.NewObject],
      timestamp: HLCTimestamp,
      allocationTransactionUUID: UUID,
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision): Future[Either[AllocationErrors.Value, AllocationRecoveryState]] = synchronized {
    
    Future.sequence { newObjects map { no =>
      val md = ObjectMetadata(ObjectRevision(allocationTransactionUUID), no.initialRefcount, timestamp)
      
      backend.allocateObject(no.newObjectUUID, md, no.objectData) map { e => e match {
        case Left(err) => Left(err)
        case Right(arr) =>
          val sp = new StorePointer(storeId.poolIndex, arr)
          val objectType = no.options match {
            case _: DataAllocationOptions => ObjectType.Data
            case _: KeyValueAllocationOptions => ObjectType.KeyValue
          }
          Right(AllocationRecoveryState.NewObject(sp, no.newObjectUUID, objectType, no.objectSize, no.objectData, no.initialRefcount))
      }}
    }} map { recoveryStateList =>
      
      val init: (Option[AllocationErrors.Value], List[AllocationRecoveryState.NewObject]) = (None, List())
      
      val (allocErrs, newObjects) = recoveryStateList.foldLeft(init) { (t, e) => e match {
        case Left(err) => (Some(err), t._2)
        case Right(no) => (t._1, no :: t._2)
      }}
      
      allocErrs match {
        case Some(err) =>
          newObjects.foreach(no => backend.deleteObject(StoreObjectID(no.newObjectUUID, no.storePointer)))
          
          Left(err)
          
        case None =>
          val ars = AllocationRecoveryState(storeId, newObjects, timestamp, allocationTransactionUUID, allocatingObject, allocatingObjectRevision)
      
          loadAllocatedObjects(ars)
      
          Right(ars)
      }
    }
  }
  
  private def loadAllocatedObjects(ars: AllocationRecoveryState): Unit = synchronized {
    ars.newObjects foreach { no =>
      val metadata = ObjectMetadata(ObjectRevision(ars.allocationTransactionUUID), no.initialRefcount, ars.timestamp)
      objectLoader.createNewObject(no.newObjectUUID, ars.allocationTransactionUUID, no.storePointer, metadata, no.objectData, no.objectType)
    }
  }
  
  def allocationResolved(ars: AllocationRecoveryState, committed: Boolean): Future[Unit] = {
    val commit = ars.newObjects.map( no => (no.newObjectUUID, committed) ).toMap
    allocationRecoveryComplete(ars, commit)
  }
    
  def allocationRecoveryComplete(ars: AllocationRecoveryState, commit: Map[UUID, Boolean]): Future[Unit] = synchronized {
    Future.sequence { commit.iterator.foldLeft(List[Future[Unit]]()) { (l, t) =>
      val (objectUUID, commit) = t
      objectLoader.getAlreadyLoadedObject(objectUUID) match {
        case None => l // Shouldn't be possible encounter this line
        
        case Some(obj) => 
          obj.completeOperation(ars.allocationTransactionUUID)
          
          // Allocation recovery could complete after transactions have already updated the object. Verify that the
          // revision matches the allocation revision before trying to commit
          if (commit && obj.revision == ObjectRevision(ars.allocationTransactionUUID))
            obj.commitBoth() :: l
          else
            l
      }
    }} map (_ => ())
  }
  
  override def getObject(pointer: ObjectPointer): Future[Either[ObjectReadError, (ObjectMetadata, DataBuffer, List[Lock])]] = pointer.getStorePointer(storeId) match {
    case None => Future.successful(Left(new InvalidLocalPointer))
    
    case Some(sp) => 
      val objectId = StoreObjectID(pointer.uuid, sp)
      val readUUID = UUID.randomUUID()
      synchronized {
        objectLoader.load(objectId, pointer.objectType, readUUID).loadBoth()
      } map { e => e match {
        case Left(err) => Left(err)
        case Right(obj) => synchronized {
          obj.completeOperation(readUUID)
          Right((obj.metadata, obj.data, obj.locks))
        }
      }}
  }
  
  
  /** Returns the object metadata but not the object data itself.
   *  This may be used to optimize reads on DataStores that separate object metadata from the data itself. Whenever read
   *  and transaction requests can be satisfied without reading the object data, this method will be used instead of
   *  getObject
   */
  override def getObjectMetadata(pointer: ObjectPointer): Future[Either[ObjectReadError, (ObjectMetadata, List[Lock])]] = pointer.getStorePointer(storeId) match {
    case None => Future.successful(Left(new InvalidLocalPointer))
    
    case Some(sp) => 
      val objectId = StoreObjectID(pointer.uuid, sp)
      val readUUID = UUID.randomUUID()
      synchronized {
        objectLoader.load(objectId, pointer.objectType, readUUID).loadMetadata()
      } map { e => e match {
        case Left(err) => Left(err)
        case Right(obj) => synchronized {
          obj.completeOperation(readUUID)
          Right((obj.metadata, obj.locks))
        }
      }}
  }
  
 
  override def getObjectData(pointer: ObjectPointer): Future[Either[ObjectReadError, (DataBuffer, List[Lock])]] = pointer.getStorePointer(storeId) match {
    case None => Future.successful(Left(new InvalidLocalPointer))
    
    case Some(sp) => 
      val objectId = StoreObjectID(pointer.uuid, sp)
      val readUUID = UUID.randomUUID()
      synchronized {
        objectLoader.load(objectId, pointer.objectType, readUUID).loadData()
      } map { e => e match {
        case Left(err) => Left(err)
        case Right(obj) => synchronized {
          obj.completeOperation(readUUID)
          Right((obj.data, obj.locks))
        }
      }}
  }
  
  def lockTransaction(txd: TransactionDescription, updateData: Option[List[LocalUpdate]]): Future[List[ObjectTransactionError]] = synchronized {
    val st = new StoreTransaction(txd, updateData.getOrElse(Nil))
    st.objectsLoaded map { _ => synchronized {
      val errors = st.checkRequirementsAndLock()
      if (errors.isEmpty)
        lockedTransactions += (txd.transactionUUID -> st)
      errors
    }}
  }
  
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[List[LocalUpdate]]): Future[Unit] = synchronized {
    // We may not have locked to this transaction due to either discovering it late or by having one or more
    // objects that didn't meet the transaction requirements. 
    val st = lockedTransactions.get(txd.transactionUUID) match {
      case Some(st) => st
      case None => 
        val st = new StoreTransaction(txd, localUpdates.getOrElse(Nil))
        lockedTransactions += (txd.transactionUUID -> st)
        st
    }
    
    st.objectsLoaded flatMap { _ => synchronized { 
      val fcommit = st.commit()
      
      fcommit foreach { _ =>
        st.releaseObjects()
      }
      
      fcommit
    }}
  }
  
  def discardTransaction(txd: TransactionDescription): Unit = synchronized { 
    lockedTransactions.get(txd.transactionUUID) foreach { st =>
      st.releaseObjects()
      lockedTransactions -= txd.transactionUUID
    }
  }
  
  // NOTE - The synchronized blocks CANNOT be used within this class. They would lock the StoreTransaction
  //        instance rather than the DataStoreFrontend instance which is what must be used for ensuring only
  //        one thread is accessing the store state. Instead, put the synchronization in the methods that
  //        use this class.
  class StoreTransaction(val txd: TransactionDescription, updateData: List[LocalUpdate]) {
    
    var locked = false
    var released = false
    
    val dataUpdates: Map[UUID, DataBuffer] = updateData.map(lu => (lu.objectUUID -> lu.data)).toMap
    
    // Filter Objects & Transaction Requirements down to just the set of objects hosted by this store
    
    val objects: Map[UUID, MutableObject] = txd.allReferencedObjectsSet.foldLeft(Map[UUID, MutableObject]())((m, op) => {
      if (op.poolUUID == storeId.poolUUID) {
        op.storePointers.find(_.poolIndex == storeId.poolIndex) match {
          case Some(sp) => m + (op.uuid -> objectLoader.load(StoreObjectID(op.uuid, sp), op.objectType, txd.transactionUUID))
          case None => m
        }
      } else
        m
    })
    
    val requirements = txd.requirements.filter(r => objects.contains(r.objectPointer.uuid))
    
    val objectsLoaded = Future.sequence {
      
      // Only load data if its needed by the transaction requirements
      val dataNeeded = requirements.foldLeft(Set[UUID]())((s, r) => r match {
        case _: DataUpdate => s + r.objectPointer.uuid
        case _ => s
      })
      
      objects.valuesIterator.map(obj => if (dataNeeded.contains(obj.objectId.objectUUID)) obj.loadBoth() else obj.loadMetadata())
      
    }.map(_ => ())
    
    def commit(): Future[Unit] = {
      
      val timestamp = HLCTimestamp(txd.startTimestamp)
      
      class CommitState(val obj: MutableObject) {
        var commitMetadata = false
        var commitData = false
        var deleteObject = false
      }
      
      var csmap = Map[UUID, CommitState]()
      
      def getCommitState(objectUUID: UUID): CommitState = csmap.get(objectUUID) match {
        case Some(cs) => cs
        case None =>
          val cs = new CommitState(objects(objectUUID))
          csmap += (objectUUID -> cs)
          cs
      }
      
      requirements.foreach { r =>
        val cs = getCommitState(r.objectPointer.uuid)
       
        // It's possible we've been asked to commit a transaction that references objects we don't have (missed the
        // creation transaction). We can safely ignore these objects and allow the repair process to clean up what
        // we miss
        if (!cs.obj.readError.isDefined) {
        
          // In order to commit each update, we must ensure that the relevant attributes are either locked
          // to this transaction or that no locks for that attribute are currently held. Otherwise we may break 
          // the contract we committed to for another concurrent transaction. The transaction leader will see that 
          // we did not vote for the commit of this transaction and will schedule a repair operation to correct
          // anything we miss updating.
          
          r match {
            case du: DataUpdate => 
              
              val canCommit = cs.obj.objectRevisionWriteLock match {
                case None => cs.obj.objectRevisionReadLocks.isEmpty
                case Some(lockedTxd) => lockedTxd.transactionUUID == txd.transactionUUID && cs.obj.objectRevisionReadLocks.isEmpty
              }
              
              if (canCommit) {
                dataUpdates.get(r.objectPointer.uuid) match {
                  case None => // Can't commit data we don't have
                    
                  case Some(data) => du.operation match {
                    case DataUpdateOperation.Overwrite => 
                      cs.obj.data = data
                      cs.obj.revision = ObjectRevision(txd.transactionUUID)
                      cs.obj.timestamp = timestamp
                      cs.commitData = true
                      cs.commitMetadata = true
                      
                    case DataUpdateOperation.Append =>
                      // Can't append if we don't have the correct pre-append object revision
                      if (cs.obj.revision == du.requiredRevision) {
                        val buf = ByteBuffer.allocate( cs.obj.data.size + data.size )
                        buf.put(cs.obj.data.asReadOnlyBuffer())
                        buf.put(data.asReadOnlyBuffer())
                        buf.position(0)
                        cs.obj.data = DataBuffer(buf)
                        cs.obj.revision = ObjectRevision(txd.transactionUUID)
                        cs.obj.timestamp = timestamp
                        cs.commitData = true
                        cs.commitMetadata = true
                      }
                  }
                }
              }
  
            case ru: RefcountUpdate => 
              val canCommit = cs.obj.objectRefcountWriteLock match {
                case None => cs.obj.objectRefcountReadLocks.isEmpty
                case Some(lockedTxd) => lockedTxd.transactionUUID == txd.transactionUUID && cs.obj.objectRefcountReadLocks.isEmpty
              }
              
              if (canCommit) {
                cs.obj.refcount = ru.newRefcount
                cs.obj.timestamp = timestamp // TODO - Do we want to update the timestamp on refcount changes?
                cs.commitMetadata = true
                if (cs.obj.refcount.count == 0)
                  cs.deleteObject = true
              }
              
            case vb: VersionBump =>
              val canCommit = cs.obj.objectRevisionWriteLock match {
                case None => cs.obj.objectRevisionReadLocks.isEmpty
                case Some(lockedTxd) => lockedTxd.transactionUUID == txd.transactionUUID && cs.obj.objectRevisionReadLocks.isEmpty
              }
              
              if (canCommit) {
                cs.obj.revision = ObjectRevision(txd.transactionUUID)
                cs.obj.timestamp = timestamp
                cs.commitMetadata = true
              }
          }
        }
      }
      
      Future.sequence { csmap.valuesIterator.map { cs =>
        if (cs.deleteObject) {
          backend.deleteObject(cs.obj.objectId)
          
        } else {
          if ( cs.commitData && cs.commitMetadata )
            cs.obj.commitBoth()
            
          else if ( cs.commitMetadata )
            cs.obj.commitMetadata()
            
          else if (cs.commitData)
            cs.obj.commitData()
            
          else
            Future.successful(())
        }
      }}.map(_ => ())
    }
    
    def lockObjects(): Unit = {
      locked = true
      
      requirements foreach { r =>
    
        val obj = objects(r.objectPointer.uuid)
        
        r match {
          case du: DataUpdate     => obj.objectRevisionWriteLock = Some(txd)
          case ru: RefcountUpdate => obj.objectRefcountWriteLock = Some(txd)
          case vb: VersionBump    => obj.objectRevisionWriteLock = Some(txd)
        }
      }
    }
    
    def releaseObjects(): Unit = if (!released) {
      released = true
      
      if (locked) {
        requirements foreach { r =>
    
          val obj = objects(r.objectPointer.uuid)
          
          r match {
            case du: DataUpdate     => obj.objectRevisionWriteLock foreach { lockedTx =>
              if (lockedTx.transactionUUID == txd.transactionUUID)
                obj.objectRevisionWriteLock = None
            }
            case ru: RefcountUpdate => obj.objectRefcountWriteLock foreach { lockedTx =>
              if (lockedTx.transactionUUID == txd.transactionUUID)
                obj.objectRefcountWriteLock = None
            }
            case vb: VersionBump    => obj.objectRevisionWriteLock foreach { lockedTx =>
              if (lockedTx.transactionUUID == txd.transactionUUID)
                obj.objectRevisionWriteLock = None
            }
          }
        }
      }
      
      objects.valuesIterator foreach { obj =>
        obj.completeOperation(txd.transactionUUID)
      }
    }
      
    def checkRequirementsAndLock(): List[ObjectTransactionError] = {
      var errors = List[ObjectTransactionError]()

      def err(e: ObjectTransactionError): Unit = errors = e :: errors
      
      import scala.language.implicitConversions
      
      implicit def mo2ptr(mo: MutableObject): ObjectPointer = txd.allReferencedObjectsSet.find(ptr => ptr.uuid == mo.objectId.objectUUID).get
      
      requirements foreach { r =>
        val obj = objects(r.objectPointer.uuid)
        
        obj.readError match {
          case Some(readErr) => err(TransactionReadError(obj, readErr))
          
          case None => r match {
            case du: DataUpdate => 
              obj.objectRevisionWriteLock match {
                case Some(lockedTxd) => err(TransactionCollision(obj, lockedTxd))
                case None =>
              }
              
              obj.objectRevisionReadLocks.foreach { t => err(TransactionCollision(obj, t._2)) }
              
              if (!dataUpdates.contains(obj.objectId.objectUUID))
                err(MissingUpdateContent(obj))
              
              if (obj.revision != du.requiredRevision)
                err(RevisionMismatch(obj, du.requiredRevision, obj.revision))
              
                
            case ru: RefcountUpdate => 
              if (obj.refcount != ru.requiredRefcount) 
                err(RefcountMismatch(obj, ru.requiredRefcount, obj.refcount))
                
              obj.objectRefcountWriteLock match {
                case Some(lockedTxd) => err(TransactionCollision(obj, lockedTxd))
                case None =>
              }
                
              obj.objectRefcountReadLocks.foreach { t => err(TransactionCollision(obj, t._2)) }
                
                
            case vb: VersionBump => 
              if (obj.revision != vb.requiredRevision)
                err(RevisionMismatch(obj, vb.requiredRevision, obj.revision))
                
              obj.objectRevisionWriteLock match {
                case Some(lockedTxd) => err(TransactionCollision(obj, lockedTxd))
                case None =>
              }
              
              obj.objectRevisionReadLocks.foreach { t => err(TransactionCollision(obj, t._2)) }
          }
        }
      }
      
      if (errors.isEmpty)
        lockObjects()
        
      errors
    }
  }
}