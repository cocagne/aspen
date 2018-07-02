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
import com.ibm.aspen.core.transaction.TransactionRequirement
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectStoreState
import com.ibm.aspen.core.transaction.RevisionLock
import com.ibm.aspen.core.allocation.AllocationOptions
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.core.objects.ObjectState
import com.ibm.aspen.core.read.FatalReadError
import com.ibm.aspen.core.objects.DataObjectState
import com.ibm.aspen.core.objects.KeyValueObjectState
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.core.objects.MetadataObjectState
import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.base.StoragePool
import com.ibm.aspen.base.MissedUpdateIterator
import scala.util.Failure
import scala.util.Success
import com.ibm.aspen.core.read.OpportunisticRebuild

object DataStoreFrontend {
  
  def appendDataBuffer(src: DataBuffer, append: DataBuffer): DataBuffer = {
    val buf = ByteBuffer.allocate( src.size + append.size )
    buf.put(src.asReadOnlyBuffer())
    buf.put(append.asReadOnlyBuffer())
    buf.position(0)
    DataBuffer(buf)
  }
  
}

class DataStoreFrontend(
    val storeId: DataStoreID,
    val backend: DataStoreBackend,
    transactionRecoveryStates: List[TransactionRecoveryState], 
    allocationRecoveryStates: List[AllocationRecoveryState]
    ) extends DataStore { 
  
  import DataStoreFrontend._
  
  override implicit val executionContext: ExecutionContext = backend.executionContext
  
  private[this] var objectLoader = new MutableObjectLoader(backend)

  private[this] var lockedTransactions = Map[UUID, StoreTransaction]()
  
  // Maps UUIDs of locked transactions to unlocked transactions that have a good chance of
  // locking if they re-attempt the lock after the locked transactions complete.
  private[this] var delayedTransactions = Map[UUID, StoreTransaction]()
  
  // maps Transaction UUIDs to the list of objects being allocated in that transaction
  private[this] var allocations = Map[UUID, List[MutableObject]]()
  
  private[this] var ofrepair: Option[Future[(StoragePool, MutableTieredKeyValueList)]] = None
  private[this] var repairing = false
  
  override val initialized: Future[DataStore] = synchronized {
    
    allocationRecoveryStates.foreach(loadAllocatedObject)
    
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
      newObjectUUID: UUID,
      options: AllocationOptions,
      objectSize: Option[Int],
      initialRefcount: ObjectRefcount,
      objectData: DataBuffer,
      timestamp: HLCTimestamp,
      allocationTransactionUUID: UUID,
      allocatingObject: ObjectPointer,
      allocatingObjectRevision: ObjectRevision): Future[Either[AllocationErrors.Value, AllocationRecoveryState]] = synchronized {
    
    val md = ObjectMetadata(ObjectRevision(allocationTransactionUUID), initialRefcount, timestamp)
        
    backend.allocateObject(newObjectUUID, md, objectData) map { e => e match {
      case Left(err) => Left(err)
      case Right(arr) =>
        val sp = new StorePointer(storeId.poolIndex, arr)
        val objectType = options match {
          case _: DataAllocationOptions => ObjectType.Data
          case _: KeyValueAllocationOptions => ObjectType.KeyValue
        }
        val ars = AllocationRecoveryState(storeId, sp, newObjectUUID, objectType, objectSize, objectData, initialRefcount, timestamp, 
                                          allocationTransactionUUID, allocatingObject, allocatingObjectRevision)
                                          
        loadAllocatedObject(ars)
        
        Right(ars)
    }}  
  }
  
  private def loadAllocatedObject(ars: AllocationRecoveryState): Unit = synchronized {
    val metadata = ObjectMetadata(ObjectRevision(ars.allocationTransactionUUID), ars.initialRefcount, ars.timestamp)
    val obj = objectLoader.createNewObject(ars.newObjectUUID, ars.allocationTransactionUUID, ars.storePointer, metadata, ars.objectData, ars.objectType)
    val lst = obj :: allocations.getOrElse(ars.allocationTransactionUUID, Nil)
    allocations += (ars.allocationTransactionUUID -> lst)
  }
  
  def allocationResolved(ars: AllocationRecoveryState, committed: Boolean): Future[Unit] = {
    allocationRecoveryComplete(ars, committed)
  }
    
  def allocationRecoveryComplete(ars: AllocationRecoveryState, commit: Boolean): Future[Unit] = synchronized {
    allocations.get(ars.allocationTransactionUUID) match {
      case None => Future.unit
      
      case Some(lst) =>  
        val flst = lst.map { mo => 

          // Allocation recovery could complete after transactions have already updated the object. Verify that the
          // revision matches the allocation revision before trying to commit
          if (commit && mo.revision == ObjectRevision(ars.allocationTransactionUUID)) {
            val fcommitted = mo.commitBoth()
            
            fcommitted onComplete {
              case _ => synchronized { mo.completeOperation(ars.allocationTransactionUUID) }
            }
            
            fcommitted
          } else {
            mo.completeOperation(ars.allocationTransactionUUID)
            Future.successful(())
          }
        }
        
        Future.sequence(flst).map(_=>())
    }
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
          //println(s"getObject ${pointer.uuid} ${obj.metadata}")
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
          //println(s"getObjectMetadata ${pointer.uuid} ${obj.metadata}")
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
          //println(s"getObjectData ${pointer.uuid} ${obj.metadata}")
          Right((obj.data, obj.locks))
        }
      }}
  }
  
  private[this] def tryLock(st: StoreTransaction): Unit = synchronized {
    if (st.checkRequirementsAndLock())
      lockedTransactions += (st.txd.transactionUUID -> st)
  }
  
  def lockTransaction(txd: TransactionDescription, updateData: Option[List[LocalUpdate]]): Future[List[ObjectTransactionError]] = synchronized {
    val st = new StoreTransaction(txd, updateData.getOrElse(Nil))
    st.objectsLoaded.foreach(_ => tryLock(st))
    st.lockPromise.future
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
      
      fcommit map { _ => synchronized {
        releaseLocks(st)
      }}
    }}
  }
  
  private[this] def releaseLocks(completedSt: StoreTransaction): Unit = {
    val transactionUUID = completedSt.txd.transactionUUID
    completedSt.releaseObjects()
    delayedTransactions.get(transactionUUID).foreach { delayedSt =>
      delayedTransactions -= transactionUUID
      delayedSt.waitingForTransactions -= transactionUUID
      if (delayedSt.waitingForTransactions.isEmpty)
        tryLock(delayedSt)
    }
  }
  
  def discardTransaction(txd: TransactionDescription): Unit = synchronized { 
    lockedTransactions.get(txd.transactionUUID) foreach { st =>
      lockedTransactions -= txd.transactionUUID
      releaseLocks(st)
    }
  }
  
  def pollAndRepairMissedUpdates(system: AspenSystem): Unit = synchronized {
    
    if (repairing)
      return
    else
      repairing = true
      
    val frepair = ofrepair match { 
      case Some(f) => f
      case None => 
        val f = for {
          pool <- system.getStoragePool(storeId.poolUUID)
          tree <- pool.getAllocationTree(system.retryStrategy)
        } yield (pool, tree)
        
        ofrepair = Some(f)
        
        f
    }
    
    for {
      (pool, tree) <- frepair
      iter = pool.createMissedUpdateIterator(storeId.poolIndex)
      _ <- repairMissedUpdates(system, tree, iter)
    } yield {
      synchronized { repairing = false }
    }
  }
  
  private def repairMissedUpdates(system: AspenSystem, tree: MutableTieredKeyValueList, iter: MissedUpdateIterator): Future[Unit] = {
    val promise = Promise[Unit]()
    
    def getObjectPointer(objectUUID: UUID): Future[Option[ObjectPointer]] = for {
      ov <- tree.get(objectUUID)
    } yield ov.map(v => ObjectPointer.fromArray(v.value))
    
    def next(): Unit = {
      iter.fetchNext() foreach { _ =>
        iter.objectUUID match {
          case None => promise.success(())
          
          case Some(objectUUID) => 
            println(s"Beginning repairs for object $objectUUID")
            val frepair = getObjectPointer(objectUUID).flatMap { optr => optr match {
              case None => throw new Exception(s"REPAIR ERROR: Failed to lookup object pointer for object $objectUUID")
                
              case Some(pointer) => repair(system, pointer).flatMap(_ => iter.markRepaired())
            }}
            
            frepair onComplete {
              case Success(_) =>
                println(s"Completed repairs of $objectUUID")
                next()
              case Failure(t) => 
                println(s"REPAIR ERROR: $t")
                next() // Could be a transient error. 
            }
        }
      }
    }
    
    next()
    
    promise.future
  }
  
  def opportunisticRebuild(message: OpportunisticRebuild): Unit = {
    println(s"Opportunistic rebuild for object ${message.objectPointer.uuid}")
    val pointer = message.objectPointer
    val objectId = StoreObjectID(pointer.uuid, pointer.getStorePointer(storeId).get)
    val repairUUID = UUID.randomUUID()
    
    val metadata = ObjectMetadata(message.newRevision, message.newRefcount, message.newTimestamp)
    val data = message.newData
    
    synchronized { objectLoader.load(objectId, pointer.objectType, repairUUID).loadBoth() } map { e => e match {
      case Left(err) => synchronized {
        backend.putObject(objectId, metadata, data)
      }
      case Right(mo) => synchronized {

        val (updateMeta, updateData) = mo match {
          case d: MutableDataObject => 
            val u = d.revision == message.oldRevision && d.refcount == message.oldRefcount
            (u, u)
            
          case k: MutableKeyValueObject =>
            val news = KeyValueObjectCodec.getUpdateSet(k.data) 
            val meta = k.revision == message.oldRevision && k.refcount == message.oldRefcount && news == message.oldUpdateSet
            
            // Only update data if the revision has changed OR the new update set is a strict superset of the updates we already
            // have. If we have received any updates outside this, we cannot overwrite our content
            val data = k.revision != message.newRevision || (KeyValueObjectCodec.getUpdateSet(mo.data) &~ news).isEmpty
            
            (meta, data)
        }
        
        if (updateMeta) {
          mo.revision = metadata.revision
          mo.refcount = metadata.refcount
          mo.timestamp = metadata.timestamp
          
          if (updateData) {
            mo.data = data
            mo match {
              case kvobj: MutableKeyValueObject => kvobj.dropKeyValueContent()
              case _ =>
            }
          }

          mo.commitBoth() onComplete { 
            case _ => synchronized { mo.completeOperation(repairUUID) } 
          }
        } else
          mo.completeOperation(repairUUID)
      }
    }} 
  }
  
  private def repair(system: AspenSystem, pointer: ObjectPointer): Future[Unit] =  {
  
    val objectId = StoreObjectID(pointer.uuid, pointer.getStorePointer(storeId).get)
    
    val foobj = system.readObject(pointer).map(Some(_)).recover{ case _: FatalReadError => None }
    
    val repairUUID = UUID.randomUUID()
    
    val folocal = synchronized { objectLoader.load(objectId, pointer.objectType, repairUUID).loadBoth() } map { e => e match {
      case Left(err) => None
      case Right(obj) => Some(obj)
    }}
    
    def isAllocated(): Future[Boolean] = {
      val key = Key(pointer.uuid)
      for {
        pool <- system.getStoragePool(pointer.poolUUID)
        tree <- pool.getAllocationTree(system.retryStrategy)
        node <- tree.fetchMutableNode(key)
      } yield node.kvos.contents.contains(key)
    }
    
    def getLocalData(os: ObjectState): DataBuffer = {
      os.getRebuildDataForStore(storeId) match {
        case None => 
          println(s"ERROR Attempted to rebuild pointer on non-hosting store")
          throw new Exception("Attempted rebuild on non hosting store")
          
        case Some(data) => data
      }
    }
    
    def fix(oobj: Option[ObjectState], olocal: Option[MutableObject]): Future[Unit] = (oobj, olocal) match {
      
      case (None, None) => isAllocated() map { allocated =>
        if (allocated)
          throw new Exception("Object is in an indeterminate state. Cannot repair yet")
      }
      
      case (None, Some(mo)) =>
        isAllocated() map { allocated => synchronized {
          if (allocated)
            throw new Exception("Object not fully deleted. Cannot repair yet")
          else if (!mo.locks.isEmpty)
            throw new Exception("Object locked. Cannot repair yet")
          else {
            println(s"Reparing missed delete for object ${pointer.uuid}")
            backend.deleteObject(objectId)
          }
        }}
        
      case (Some(os), None) => synchronized {
        val metadata = ObjectMetadata(os.revision, os.refcount, os.timestamp)
        val data = getLocalData(os)
        println(s"Reparing missed allocation for object ${pointer.uuid}")
        backend.putObject(objectId, metadata, data)
      }
      
      case (Some(os), Some(mo)) => synchronized { 
        val update = os match {
          case d: DataObjectState => 
            d.revision != mo.revision || d.refcount != mo.refcount
            
          case kvos: KeyValueObjectState => 
            kvos.revision != mo.revision || kvos.refcount != mo.refcount || KeyValueObjectCodec.getUpdateSet(mo.data) != kvos.updates
            
          case _: MetadataObjectState => true // Should not be possible
        }
        
        if (update) {
          mo.revision = os.revision
          mo.refcount = os.refcount
          mo.timestamp = os.timestamp
          mo.data = getLocalData(os)
          mo match {
            case kvobj: MutableKeyValueObject => kvobj.dropKeyValueContent()
            case _ =>
          }
          println(s"Reparing missed update for object ${pointer.uuid}")
          mo.commitBoth()
        } else
          Future.unit
      }
    }
    
    for {
      oobj <- foobj
      olocal <- folocal
      _ <- fix(oobj, olocal)
    } yield {
      olocal.foreach(mo => synchronized { mo.completeOperation(repairUUID) })
    }
  }
  
  // NOTE - The synchronized blocks CANNOT be used within this class. They would lock the StoreTransaction
  //        instance rather than the DataStoreFrontend instance which is what must be used for ensuring only
  //        one thread is accessing the store state. Instead, put the synchronization in the methods that
  //        use this class.
  class StoreTransaction(val txd: TransactionDescription, updateData: List[LocalUpdate]) {
    
    var locked = false
    var released = false
    
    var waitingForTransactions = Set[UUID]()
    
    val lockPromise = Promise[List[ObjectTransactionError]]()
    
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
        case _: KeyValueUpdate => s + r.objectPointer.uuid
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
      
      requirements.foreach { requirement =>
        val cs = getCommitState(requirement.objectPointer.uuid)
       
        // It's possible we've been asked to commit a transaction that references objects we don't have (missed the
        // creation transaction). We can safely ignore these objects and allow the repair process to clean up what
        // we miss
        if (!cs.obj.readError.isDefined) {
        
          // Before committing the updates associated with each transaction requirement, we must first ensure
          // that the requirement is met. We may be committing a transaction that we didn't vote to commit due
          // to a problem with one or more of the requirements. Commit the ones that match and skip those that
          // do not. 
          
          requirement match {
            case du: DataUpdate => if ( getRequirementErrors(du).isEmpty ) {
              val data = dataUpdates(requirement.objectPointer.uuid)
                  
              du.operation match {
                case DataUpdateOperation.Overwrite => cs.obj.data = data
                case DataUpdateOperation.Append    => cs.obj.data = appendDataBuffer(cs.obj.data, data)
              }
              
              cs.obj.revision = ObjectRevision(txd.transactionUUID)
              cs.obj.timestamp = timestamp
              cs.commitData = true
              cs.commitMetadata = true
            }
  
            case ru: RefcountUpdate => if ( getRequirementErrors(ru).isEmpty ) {
              cs.obj.refcount = ru.newRefcount
              cs.obj.timestamp = timestamp // TODO - Do we want to update the timestamp on refcount changes?
              cs.commitMetadata = true
              if (cs.obj.refcount.count == 0)
                cs.deleteObject = true
            } 
              
            case vb: VersionBump => if ( getRequirementErrors(vb).isEmpty ) {
              cs.obj.revision = ObjectRevision(txd.transactionUUID)
              cs.obj.timestamp = timestamp
              cs.commitMetadata = true
            }
            
            case _: RevisionLock => // Nothing to do
            
            case kv: KeyValueUpdate => if ( getRequirementErrors(kv).isEmpty ) {
              val kvobj = cs.obj.asInstanceOf[MutableKeyValueObject]
              val data = dataUpdates(requirement.objectPointer.uuid)
              cs.commitData = true
              
              kv.requiredRevision.foreach { _ =>
                cs.obj.revision = ObjectRevision(txd.transactionUUID)
                cs.obj.timestamp = timestamp
                cs.commitMetadata = true 
              }
              
              kv.updateType match {
                case KeyValueUpdate.UpdateType.Overwrite =>
                  cs.obj.data = data
                  kvobj.dropKeyValueContent() // Object content will need to be parsed from the new data
                  
                case KeyValueUpdate.UpdateType.Append    => 
                  cs.obj.data = appendDataBuffer(cs.obj.data, data)
                  kvobj.updateKeyValueContent(data) // Updates the already-parsed data (if it's been parsed) with the new values
              }
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
          case rl: RevisionLock   => obj.objectRevisionReadLocks += (txd.transactionUUID -> txd)
          case kv: KeyValueUpdate =>
            val kvobj = obj.asInstanceOf[MutableKeyValueObject]
            
            kv.requiredRevision match {
              // If we're locking the revision, there's no need to lock each key separately
              case Some(_) => obj.objectRevisionWriteLock = Some(txd)
              
              case None =>
                kv.requirements.foreach { req => 
                  kvobj.keyRevisionWriteLocks += (req.key -> txd)
                }    
            }
        }
      }
    }
    
    def releaseObjects(): Unit = if (!released) {
      released = true
      
      if (locked) {
        requirements foreach { r =>
    
          val obj = objects(r.objectPointer.uuid)
          
          r match {
            case du: DataUpdate     => obj.objectRevisionWriteLock = None
            
            case ru: RefcountUpdate => obj.objectRefcountWriteLock = None
            
            case vb: VersionBump    => obj.objectRevisionWriteLock = None
            
            case rl: RevisionLock => obj.objectRevisionReadLocks -= txd.transactionUUID
            
            case kv: KeyValueUpdate =>
              val kvobj = obj.asInstanceOf[MutableKeyValueObject]
              
              kv.requiredRevision match { 
                case Some(_) => obj.objectRevisionWriteLock = None
                
                case None => 
                  kv.requirements.foreach { req => 
                    kvobj.keyRevisionWriteLocks -= req.key
                  }
              }
          }
        }
      }
      
      objects.valuesIterator foreach { obj =>
        obj.completeOperation(txd.transactionUUID)
      }
    }
    
    def getRequirementErrors(requirement: TransactionRequirement): List[ObjectTransactionError] = {
      var errors = List[ObjectTransactionError]()
      
      def err(e: ObjectTransactionError): Unit = errors = e :: errors
      
      import scala.language.implicitConversions
      
      implicit def mo2ptr(mo: MutableObject): ObjectPointer = txd.allReferencedObjectsSet.find(ptr => ptr.uuid == mo.objectId.objectUUID).get
      
      val obj = objects(requirement.objectPointer.uuid)
      

      obj.readError match {
        case Some(readErr) => err(TransactionReadError(obj, readErr))
        
        case None => requirement match {
          case du: DataUpdate =>
            obj.getTransactionPreventingRevisionWriteLock(txd) foreach { lockedTxd => err(TransactionCollision(obj, lockedTxd)) }
            
            if (obj.revision != du.requiredRevision)
              err(RevisionMismatch(obj, du.requiredRevision, obj.revision))
              
            dataUpdates.get(obj.objectId.objectUUID) match {
              case None => err(MissingUpdateContent(obj))
              
              case Some(data) =>
                val haveSpace = du.operation match {
                  case DataUpdateOperation.Overwrite => backend.haveFreeSpaceForOverwrite(obj.objectId, obj.data.size, data.size)
                  case DataUpdateOperation.Append    => backend.haveFreeSpaceForAppend(obj.objectId, obj.data.size, obj.data.size + data.size)
                }
                if (!haveSpace)
                  err(InsufficientFreeSpace(obj))
            }
            
              
          case ru: RefcountUpdate =>
            obj.getTransactionPreventingRefcountWriteLock(txd) foreach { lockedTxd => err(TransactionCollision(obj, lockedTxd)) }
            
            if (obj.refcount != ru.requiredRefcount) 
              err(RefcountMismatch(obj, ru.requiredRefcount, obj.refcount))  
              
              
          case vb: VersionBump =>
            obj.getTransactionPreventingRevisionWriteLock(txd) foreach { lockedTxd => err(TransactionCollision(obj, lockedTxd)) }
            
            if (obj.revision != vb.requiredRevision)
              err(RevisionMismatch(obj, vb.requiredRevision, obj.revision))
              
          case rl: RevisionLock =>
            obj.getTransactionPreventingRevisionReadLock(txd) foreach { lockedTxd => err(TransactionCollision(obj, lockedTxd)) }
            
            if (obj.revision != rl.requiredRevision)
              err(RevisionMismatch(obj, rl.requiredRevision, obj.revision))
              
          case kv: KeyValueUpdate => 
            obj match {
              case kvobj: MutableKeyValueObject =>
                
                kv.requiredRevision.foreach { requiredRevision =>
                  obj.getTransactionPreventingRevisionWriteLock(txd) foreach { lockedTxd => err(TransactionCollision(obj, lockedTxd)) }
                  
                  if (obj.revision != requiredRevision)
                    err(RevisionMismatch(obj, requiredRevision, obj.revision))
                }
                
                dataUpdates.get(obj.objectId.objectUUID) match {
                  case None => err(MissingUpdateContent(obj))
                  
                  case Some(data) =>
                    val haveSpace = kv.updateType match {
                      case KeyValueUpdate.UpdateType.Overwrite =>
                        val meetsSizeRequirement = requirement.objectPointer.size match {
                          case None => true
                          case Some(maxSize) => data.size <= maxSize
                        }
                        meetsSizeRequirement && backend.haveFreeSpaceForOverwrite(obj.objectId, obj.data.size, data.size)
                        
                      case KeyValueUpdate.UpdateType.Append    => 
                        val meetsSizeRequirement = requirement.objectPointer.size match {
                          case None => true
                          case Some(maxSize) => (obj.data.size + data.size) <= maxSize
                        }
                        meetsSizeRequirement && backend.haveFreeSpaceForAppend(obj.objectId, obj.data.size, obj.data.size + data.size)
                    }
                    
                    if (!haveSpace)
                      err(InsufficientFreeSpace(obj))
                }
                
                if (!kv.requirements.isEmpty) {
                  kvobj.parseKeyValueContent()
                
                  val objectLocked = kvobj.objectRevisionWriteLock match {
                    case None => false
                    case Some(lockedTxd) => lockedTxd.transactionUUID != txd.transactionUUID
                  }
                  
                  if (objectLocked) {
                    err(TransactionCollision(obj, kvobj.objectRevisionWriteLock.get))
                  } else {
                    kv.requirements.foreach { req => 
                      val ov = kvobj.idaEncodedContents.get(req.key)

                      kvobj.keyRevisionWriteLocks.get(req.key) foreach { lockedTxd =>
                        if (lockedTxd.transactionUUID != txd.transactionUUID)
                          err(KeyValueRequirementError(obj, req.key))
                      }
                      
                      req.tsRequirement match {
                        case KeyValueUpdate.TimestampRequirement.Equals => ov match {
                          case None => err(KeyValueRequirementError(obj, req.key))
                          case Some(v) => if (v.timestamp != req.timestamp) err(KeyValueRequirementError(obj, req.key))
                        }
                        case KeyValueUpdate.TimestampRequirement.LessThan => ov match {
                          case None => err(KeyValueRequirementError(obj, req.key))
                          case Some(v) => if (req.timestamp.asLong >= v.timestamp.asLong) err(KeyValueRequirementError(obj, req.key))
                        }
                        case KeyValueUpdate.TimestampRequirement.Exists => ov match {
                          case None => err(KeyValueRequirementError(obj, req.key))
                          case Some(v) => 
                        }
                        case KeyValueUpdate.TimestampRequirement.DoesNotExist => ov match {
                          case None => 
                          case Some(v) => err(KeyValueRequirementError(obj, req.key))
                        }
                      }
                    }
                  }
                }

              case _ => err(InvalidObjectType(obj))
            }
            
        }
      }
      
      errors
    }
      
    
    def checkRequirementsAndLock(): Boolean = {
      val errors = requirements.flatMap( requirement => getRequirementErrors(requirement) ) 
      
      if (errors.isEmpty)
        lockObjects()
      else {
        //println(s"**** ERRORS IN TRANSACTION ${txd.transactionUUID}")
        //errors.foreach(err => println(s"   $err"))
        
        val collisions = errors.foldLeft(Map[UUID,UUID]()) { (m, e) => e match {
          case c: TransactionCollision => m + (c.objectPointer.uuid -> c.lockedTransaction.transactionUUID)
          case _ => m
        }}
        
        val mismatches = errors.foldLeft(Set[UUID]()) { (s, e) => e match {
          case r: RevisionMismatch => s + r.objectPointer.uuid
          case _ => s
        }}
        
        val probablyMissedCommitOfLockedTx = errors.forall { e => e match {
          case r: RevisionMismatch => collisions.get(r.objectPointer.uuid) match {
            case None => false
            case Some(lockedRev) => r.required.lastUpdateTxUUID == lockedRev
          }
          
          case c: TransactionCollision => mismatches.contains(c.objectPointer.uuid) 
          
          case _ => false
        }}
        
        if (probablyMissedCommitOfLockedTx) {
          collisions.values.foreach { lockedTxUUID => 
            waitingForTransactions += lockedTxUUID
            delayedTransactions += (lockedTxUUID -> this)
          }
        }
      }
    
      if (waitingForTransactions.isEmpty)
        lockPromise.success(errors)
        
      waitingForTransactions.isEmpty && errors.isEmpty
    }
  }
}