package com.ibm.aspen.core.data_store

import java.util.UUID

import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.base.{AspenSystem, StoragePool}
import com.ibm.aspen.core.allocation._
import com.ibm.aspen.core.objects._
import com.ibm.aspen.core.read.OpportunisticRebuild
import com.ibm.aspen.core.transaction._
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import org.apache.logging.log4j.{LogManager, Logger}

import scala.concurrent.{ExecutionContext, Future, Promise}

class DataStoreFrontend(
    val storeId: DataStoreID,
    val backend: DataStoreBackend,
    transactionRecoveryStates: List[TransactionRecoveryState], 
    allocationRecoveryStates: List[AllocationRecoveryState]
    ) extends DataStore { 
  
  object log {
    val alloc:Logger  = LogManager.getLogger(this.getClass.getName + ".alloc")
    val read:Logger  = LogManager.getLogger(this.getClass.getName + ".read")
    val tx:Logger  = LogManager.getLogger(this.getClass.getName + ".tx")
  }
  
  override implicit val executionContext: ExecutionContext = backend.executionContext

  def allOperationsCompleted(o: StoreObjectState): Unit = ???
  def failedToRead(o: StoreObjectState, err: ObjectReadError): Unit = ???
  def loadedObjectState(ometa: Option[ObjectMetadata], odata: Option[DataBuffer]): Unit = ???

  // TODO FIX REBUILD
  def opportunisticRebuild(message: OpportunisticRebuild): Unit = {}
  def pollAndRepairMissedUpdates(system: AspenSystem): Unit = {}

  private[data_store] def executeInSynchronizedBlock[T](fn: => T): T = synchronized { fn }

  private[this] val objectLoader = new MutableObjectLoader(this, backend)

  // The content of the following two maps are managed by instances of the StoreTransaction class
  private[this] var activeTransactions = Map[UUID, StoreTransaction]()
  private[this] var lockedTransactions = Map[UUID, StoreTransaction]()
  
  // Maps UUIDs of locked transactions to unlocked transactions that have a good chance of
  // locking if they re-attempt the lock after the locked transactions complete.
  private[this] var delayedTransactions = Map[UUID, Set[StoreTransaction]]()
  
  // maps Transaction UUIDs to the list of objects being allocated in that transaction
  private[this] var allocations = Map[UUID, List[MutableObject]]()
  
  override val initialized: Future[DataStore] = synchronized {
    
    allocationRecoveryStates.foreach(loadAllocatedObject)
    
    val ftxrecovery = transactionRecoveryStates.foldLeft(List[Future[Unit]]()) { (l, trs) => 
      val t = new StoreTransaction(trs.txd, trs.localUpdates.getOrElse(Nil))
      
      if (trs.disposition == TransactionDisposition.VoteCommit) {
        // Re-establish locks on all transactions we voted to commit
        t.lockObjects()
      }
      
      t.objectsLoaded :: l
    }

    Future.sequence(ftxrecovery).map(_=> this)
  }
  
  override def close(): Future[Unit] = synchronized {
    backend.close()
  }
  
  override def bootstrapAllocateNewObject(objectUUID: UUID, initialContent: DataBuffer, timestamp: HLCTimestamp): Future[StorePointer] = synchronized {
    val metadata = ObjectMetadata(ObjectRevision(new UUID(0,0)), ObjectRefcount(0,1), timestamp)
    
    backend.allocateObject(objectUUID, metadata, initialContent) flatMap {
      case Left(err) => throw new Exception(s"Allocation failed: $err")
      case Right(arr) => 
        val sp = new StorePointer(storeId.poolIndex, arr)
        backend.putObject(StoreObjectID(objectUUID, sp), metadata, initialContent).map( _ => sp )
    }
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
    
    val objectType = options match {
      case _: DataAllocationOptions => ObjectType.Data
      case _: KeyValueAllocationOptions => ObjectType.KeyValue
    }
        
    log.alloc.info(s"$storeId alloc tx $allocationTransactionUUID for $objectType object $newObjectUUID")
    
    val md = ObjectMetadata(ObjectRevision(allocationTransactionUUID), initialRefcount, timestamp)
        
    backend.allocateObject(newObjectUUID, md, objectData) map {
      case Left(err) => Left(err)
      case Right(arr) =>
        val sp = new StorePointer(storeId.poolIndex, arr)
        
        val ars = AllocationRecoveryState(storeId, sp, newObjectUUID, objectType, objectSize, objectData, initialRefcount, timestamp, 
                                          allocationTransactionUUID, allocatingObject, allocatingObjectRevision)
                                          
        loadAllocatedObject(ars)
        
        Right(ars)
    }
  }
  
  private def loadAllocatedObject(ars: AllocationRecoveryState): Unit = synchronized {
    val metadata = ObjectMetadata(ObjectRevision(ars.allocationTransactionUUID), ars.initialRefcount, ars.timestamp)
    val obj = objectLoader.createNewObject(ars.newObjectUUID, ars.allocationTransactionUUID, ars.timestamp, ars.storePointer, metadata, ars.objectData, ars.objectType)
    val lst = obj :: allocations.getOrElse(ars.allocationTransactionUUID, Nil)
    allocations += (ars.allocationTransactionUUID -> lst)
  }
  
  def allocationResolved(ars: AllocationRecoveryState, committed: Boolean): Future[Unit] = {
    log.alloc.info(s"$storeId alloc tx ${ars.allocationTransactionUUID} resolved. Committed = $committed")
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
            
            fcommitted.onComplete(_ => mo.completeOperation(ars.allocationTransactionUUID))

            fcommitted
          } else {
            mo.completeOperation(ars.allocationTransactionUUID)
            Future.successful(())
          }
        }
        
        Future.sequence(flst).map(_=>())
    }
  }
  
  override def getObject(pointer: ObjectPointer): Future[Either[ObjectReadError, (ObjectMetadata, DataBuffer, List[Lock], Set[UUID])]] = pointer.getStorePointer(storeId) match {
    case None =>
      log.read.info(s"$storeId read INVALID object ${pointer.uuid}")
      Future.successful(Left(new InvalidLocalPointer))
    
    case Some(sp) => 
      val objectId = StoreObjectID(pointer.uuid, sp)
      val readUUID = UUID.randomUUID()
      val fload = objectLoader.load(objectId, pointer.objectType, readUUID).loadBoth()

      fload.map {
        case Left(err) =>
          log.read.info(s"$storeId read object ${pointer.uuid}. Error $err")
          Left(err)
          
        case Right(obj) =>
          log.read.info(s"$storeId read object ${pointer.uuid}. Rev ${obj.revision} TS ${obj.timestamp} Len ${obj.data.size}")
          obj.completeOperation(readUUID)

          synchronized {
            if (obj.deleted)
              Left(new InvalidLocalPointer)
            else
              Right((obj.metadata, obj.data, obj.locks, obj.writeLocks))
          }
      }
  }
  
  
  /** Returns the object metadata but not the object data itself.
   *  This may be used to optimize reads on DataStores that separate object metadata from the data itself. Whenever read
   *  and transaction requests can be satisfied without reading the object data, this method will be used instead of
   *  getObject
   */
  override def getObjectMetadata(pointer: ObjectPointer): Future[Either[ObjectReadError, (ObjectMetadata, List[Lock], Set[UUID])]] = pointer.getStorePointer(storeId) match {
    case None =>
      log.read.info(s"$storeId readMeta INVALID object ${pointer.uuid}")
      Future.successful(Left(new InvalidLocalPointer))
    
    case Some(sp) => 
      val objectId = StoreObjectID(pointer.uuid, sp)
      val readUUID = UUID.randomUUID()
      val fload = objectLoader.load(objectId, pointer.objectType, readUUID).loadMetadata()
      
      fload.map {
        case Left(err) =>
          log.read.info(s"$storeId readMeta object ${pointer.uuid}. Error $err")
          Left(err)
        case Right(obj) =>
          log.read.info(s"$storeId readMeta object ${pointer.uuid}. Rev ${obj.revision} TS ${obj.timestamp}")
          obj.completeOperation(readUUID)

          synchronized {
            if (obj.deleted)
              Left(new InvalidLocalPointer)
            else
              Right((obj.metadata, obj.locks, obj.writeLocks))
          }
      }
  }
  

  private[this] def getStoreTransaction(txd: TransactionDescription, updateData: Option[List[LocalUpdate]]): StoreTransaction = {
    activeTransactions.getOrElse(txd.transactionUUID, new StoreTransaction(txd, updateData.getOrElse(Nil)))
  }
  
  def lockTransaction(txd: TransactionDescription, updateData: Option[List[LocalUpdate]]): Future[List[ObjectTransactionError]] = synchronized {
    val p = Promise[List[ObjectTransactionError]]()
    
    val st = getStoreTransaction(txd, updateData)
    
    st.objectsLoaded.foreach(_ => st.checkRequirementsAndLock(Some(p)))
    
    p.future
  }
  
  def discardTransaction(txd: TransactionDescription): Unit = synchronized { 
    activeTransactions.get(txd.transactionUUID).foreach(st => st.discard())
  }
  
  def commitTransactionUpdates(txd: TransactionDescription, localUpdates: Option[List[LocalUpdate]]): Future[Unit] = synchronized {
    val st = getStoreTransaction(txd, localUpdates)
    
    st.objectsLoaded flatMap { _ => 
      synchronized { 
        st.commit().map( _ => st.releaseObjects() )
      }
    }
  }
  

  
  // NOTE - The synchronized blocks CANNOT be used within this class. They would lock the StoreTransaction
  //        instance rather than the DataStoreFrontend instance which is what must be used for ensuring only
  //        one thread is accessing the store state. Instead, put the synchronization in the methods that
  //        use this class.
  class StoreTransaction(val txd: TransactionDescription, updateData: List[LocalUpdate]) {
    
    log.tx.info(s"$storeId tx: ${txd.transactionUUID} Beginning transaction")
    
    activeTransactions += (txd.transactionUUID -> this)
    
    var locked = false
    var committed = false
    
    val dataUpdates: Map[UUID, DataBuffer] = updateData.map(lu => lu.objectUUID -> lu.data).toMap
    
    var lockRequests: List[Promise[List[ObjectTransactionError]]] = Nil
    
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
    
    log.tx.info(s"$storeId tx: ${txd.transactionUUID} Local objects: ${objects.keys.toList}")
    
    val requirements: List[TransactionRequirement] = txd.requirements.filter(r => objects.contains(r.objectPointer.uuid))
    
    val objectsLoaded: Future[Unit] = Future.sequence {
      
      // Only load data if its needed by the transaction requirements
      val dataNeeded = requirements.foldLeft(Set[UUID]())((s, r) => r match {
        case _: DataUpdate => s + r.objectPointer.uuid
        case _: KeyValueUpdate => s + r.objectPointer.uuid
        case _ => s
      })
      
      objects.valuesIterator.map(obj => if (dataNeeded.contains(obj.objectId.objectUUID)) obj.loadBoth() else obj.loadMetadata())
      
    }.map(_ => ())
    
    def commit(): Future[Unit] = if (committed) 
      Future.unit
    else {
      
      log.tx.info(s"$storeId tx: ${txd.transactionUUID} Committing")
      committed = true
      
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
        if (cs.obj.readError.isEmpty) {
        
          // Before committing the updates associated with each transaction requirement, we must first ensure
          // that the requirement is met. We may be committing a transaction that we didn't vote to commit due
          // to a problem with one or more of the requirements. Commit the ones that match and skip those that
          // do not. The repair process will eventually fix them.
          
          val requirementErrors = getRequirementErrors(requirement)
          
          if (requirementErrors.isEmpty) {
            requirement match {
              case du: DataUpdate => 
                val data = dataUpdates(requirement.objectPointer.uuid)
                    
                du.operation match {
                  case DataUpdateOperation.Overwrite => cs.obj match {
                    case d: MutableDataObject => d.overwriteData(data)
                    case _: MutableKeyValueObject => log.tx.info(s"$storeId tx: ${txd.transactionUUID} Invalid Overwrite on key-value object ${requirement.objectPointer.uuid}")
                  }
                  case DataUpdateOperation.Append => cs.obj match {
                    case d: MutableDataObject => d.appendData(data)
                    case _: MutableKeyValueObject => log.tx.info(s"$storeId tx: ${txd.transactionUUID} Invalid Append on key-value object ${requirement.objectPointer.uuid}")
                  }
                }
                cs.obj.setMetadata(cs.obj.metadata.copy(revision=ObjectRevision(txd.transactionUUID), timestamp=timestamp))
                cs.commitData = true
                cs.commitMetadata = true
                log.tx.info(s"$storeId tx: ${txd.transactionUUID} Committing DataUpdate ${du.operation} for object ${requirement.objectPointer.uuid}")
              
              case ru: RefcountUpdate =>
                // TODO - Do we want to update the timestamp on refcount changes?
                cs.obj.setMetadata(cs.obj.metadata.copy(refcount=ru.newRefcount, timestamp=timestamp))
                cs.commitMetadata = true
                if (cs.obj.refcount.count == 0) {
                  cs.deleteObject = true
                  log.tx.info(s"$storeId tx: ${txd.transactionUUID} Committing RefcountUpdate to ${ru.newRefcount} for object ${requirement.objectPointer.uuid}")
                } else {
                  log.tx.info(s"$storeId tx: ${txd.transactionUUID} Committing RefcountUpdate to DELETE object ${requirement.objectPointer.uuid}")
                }
              
              case _: VersionBump =>
                cs.obj.setMetadata(cs.obj.metadata.copy(revision=ObjectRevision(txd.transactionUUID), timestamp=timestamp))
                cs.commitMetadata = true
                log.tx.info(s"$storeId tx: ${txd.transactionUUID} Committing VersionBump for object ${requirement.objectPointer.uuid}")
              
              case _: RevisionLock => // Nothing to do
              
              case kv: KeyValueUpdate =>
                val kvobj = cs.obj.asInstanceOf[MutableKeyValueObject]
                val data = dataUpdates(requirement.objectPointer.uuid)
                cs.commitData = true
                
                kv.requiredRevision.foreach { _ =>
                  cs.obj.setMetadata(cs.obj.metadata.copy(revision=ObjectRevision(txd.transactionUUID), timestamp=timestamp))
                  cs.commitMetadata = true 
                }
                
                log.tx.info(s"$storeId tx: ${txd.transactionUUID} Committing KeyValue ${kv.updateType} for object ${requirement.objectPointer.uuid}")
                
                kv.updateType match {
                  case KeyValueUpdate.UpdateType.Update => kvobj.update(data, ObjectRevision(txd.transactionUUID), timestamp)
                }
            }
          } else {
            if (log.tx.isWarnEnabled()) {
              log.tx.warn(s"$storeId tx: ${txd.transactionUUID} SKIPPING commit for operation ${requirement.getClass.getSimpleName} on object ${requirement.objectPointer.uuid} due to errors:")
              requirementErrors.foreach( err => log.tx.warn(s"$storeId tx: ${txd.transactionUUID} ERROR: $err") )
            }
          }
        }
      }
      
      Future.sequence { csmap.valuesIterator.map { cs =>
        if (cs.deleteObject) {
          cs.obj.deleted = true
          backend.deleteObject(cs.obj.objectId)
          
        } else {
          if ( cs.commitData && cs.commitMetadata )
            cs.obj.commitBoth()
            
          else if ( cs.commitMetadata )
            cs.obj.commitMetadata()

          else
            Future.successful(())
        }
      }}.map(_ => ())
    }
    
    def lockObjects(): Unit = if (!committed && !locked) {
      log.tx.info(s"$storeId tx: ${txd.transactionUUID} Locking to transaction")
      
      locked = true
      lockedTransactions += (txd.transactionUUID -> this)
      
      requirements foreach { r =>
    
        val obj = objects(r.objectPointer.uuid)
        
        r match {
          case _: DataUpdate     => obj.objectRevisionWriteLock = Some(txd)
          case _: RefcountUpdate => obj.objectRefcountWriteLock = Some(txd)
          case _: VersionBump    => obj.objectRevisionWriteLock = Some(txd)
          case _: RevisionLock   => obj.objectRevisionReadLocks += (txd.transactionUUID -> txd)
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
    
    def unblockDelayedTransactions(): Unit = {
      // Attempt to lock any transactions that were dependent upon this one completing
      delayedTransactions.get(txd.transactionUUID).foreach { delayedSet =>
        delayedTransactions -= txd.transactionUUID
        delayedSet.foreach( st => st.checkRequirementsAndLock(None) )
      }
    }
    
    def discard(): Unit = {
      releaseObjects()
      activeTransactions -= txd.transactionUUID
      
      unblockDelayedTransactions()
      
      log.tx.info(s"$storeId tx: ${txd.transactionUUID} Discarding transaction")
    }
    
    def releaseObjects(): Unit = if (locked) {
      locked = false
    
      log.tx.info(s"$storeId tx: ${txd.transactionUUID} Unlocking from transaction")
      
      lockedTransactions -= txd.transactionUUID
      
      requirements foreach { r =>
  
        val obj = objects(r.objectPointer.uuid)
        
        r match {
          case _: DataUpdate     => obj.objectRevisionWriteLock = None
          
          case _: RefcountUpdate => obj.objectRefcountWriteLock = None
          
          case _: VersionBump    => obj.objectRevisionWriteLock = None
          
          case _: RevisionLock => obj.objectRevisionReadLocks -= txd.transactionUUID
          
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
    
      objects.valuesIterator foreach { obj =>
        obj.completeOperation(txd.transactionUUID)
      }
      
      unblockDelayedTransactions()
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
                      case KeyValueUpdate.UpdateType.Update =>
                        
                        val meetsSizeRequirement = requirement.objectPointer.size match {
                          case None => true
                          case Some(maxSize) => 
                            val kvoss = obj.asInstanceOf[MutableKeyValueObject].storeState
    
                            val updatedKvoss = kvoss.update(data, ObjectRevision(txd.transactionUUID), HLCTimestamp(txd.startTimestamp))
                            
                            updatedKvoss.encodedSize <= maxSize
                        }
                        meetsSizeRequirement && backend.haveFreeSpaceForAppend(obj.objectId, obj.data.size, obj.data.size + data.size)
                    }
                    
                    if (!haveSpace)
                      err(InsufficientFreeSpace(obj))
                }
                
                if (kv.requirements.nonEmpty) {
                  
                  val kvoss = kvobj.storeState
                  
                  val objectLocked = kvobj.objectRevisionWriteLock match {
                    case None => false
                    case Some(lockedTxd) => lockedTxd.transactionUUID != txd.transactionUUID
                  }
                  
                  if (objectLocked) {
                    err(TransactionCollision(obj, kvobj.objectRevisionWriteLock.get))
                  } else {
                    kv.requirements.foreach { req => 
                      val ov = kvoss.idaEncodedContents.get(req.key)

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
                          case Some(_) =>
                        }
                        case KeyValueUpdate.TimestampRequirement.DoesNotExist => ov match {
                          case None => 
                          case Some(_) => err(KeyValueRequirementError(obj, req.key))
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
      
    
    def checkRequirementsAndLock(op: Option[Promise[List[ObjectTransactionError]]]): Unit = {
      if (locked)
        op.foreach(p => p.success(Nil))
      else {
        var waitingForTransactions = Set[UUID]()
        
        val errors = requirements.flatMap( requirement => getRequirementErrors(requirement) ) 
        
        if (errors.isEmpty)
          lockObjects()
        else {
          
          if (log.tx.isWarnEnabled()) {
            log.tx.warn(s"$storeId tx: ${txd.transactionUUID} not locking due to errors:")
            errors.foreach( err => log.tx.warn(s"$storeId tx: ${txd.transactionUUID} ERROR: $err") )
          }
//          if (true) {
//            println(s"$storeId tx: ${txd.transactionUUID} not locking due to errors:")
//            errors.foreach( err => println(s"$storeId tx: ${txd.transactionUUID} ERROR: $err") )
//          }
          
          val collisions = errors.foldLeft(Map[UUID,UUID]()) { (m, e) => e match {
            case c: TransactionCollision => m + (c.objectPointer.uuid -> c.lockedTransaction.transactionUUID)
            case _ => m
          }}
          
          val mismatches = errors.foldLeft(Set[UUID]()) { (s, e) => e match {
            case r: RevisionMismatch => s + r.objectPointer.uuid
            case _ => s
          }}
          
          val probablyMissedCommitOfLockedTx = errors.forall {
            case r: RevisionMismatch => collisions.get(r.objectPointer.uuid) match {
              case None => false
              case Some(lockedRev) => r.required.lastUpdateTxUUID == lockedRev
            }
            
            case c: TransactionCollision => mismatches.contains(c.objectPointer.uuid) 
            
            case _ => false
          }
          
          if (probablyMissedCommitOfLockedTx) {
            collisions.values.foreach { lockedTxUUID =>
              val dset = delayedTransactions.getOrElse(lockedTxUUID, Set()) + this
              
              waitingForTransactions += lockedTxUUID
              delayedTransactions += (lockedTxUUID -> dset)
            }
          }
        }
      
        // limit pending requests to prevent memory explosion if one of the dependent transactions takes
        // a very long time to complete
        if (waitingForTransactions.isEmpty || lockRequests.size > 5) {
          op.foreach(p => p.success(errors))
          lockRequests.foreach(p => p.success(errors))
          lockRequests = Nil
        } else {
          log.tx.info(s"$storeId tx: ${txd.transactionUUID} delaying action until transactions complete: $waitingForTransactions")
          op.foreach { p =>
            lockRequests = p :: lockRequests
          }
        }
      }
    }
  }
}