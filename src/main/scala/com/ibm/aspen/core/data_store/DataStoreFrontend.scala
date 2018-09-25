package com.ibm.aspen.core.data_store

import java.util.UUID

import com.ibm.aspen.base.AspenSystem
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


  // TODO FIX REBUILD
  def opportunisticRebuild(message: OpportunisticRebuild): Unit = {}
  def pollAndRepairMissedUpdates(system: AspenSystem): Unit = {}


  // The content of the following two maps are managed by instances of the StoreTransaction class
  protected[data_store] var activeTransactions = Map[UUID, StoreTransaction]()
  protected[data_store] var lockedTransactions = Map[UUID, StoreTransaction]()
  
  // Maps UUIDs of locked transactions to unlocked transactions that have a good chance of
  // locking if they re-attempt the lock after the locked transactions complete.
  protected[data_store] var delayedTransactions = Map[UUID, Set[StoreTransaction]]()
  
  // maps Transaction UUIDs to the list of objects being allocated in that transaction
  private[this] var allocations = Map[UUID, List[ObjectStoreState]]()

  // maps Object UUID to StoreObjectState
  private[this] var loadedObjects = Map[UUID, ObjectStoreState]()
  
  override val initialized: Future[DataStore] = synchronized {
    
    allocationRecoveryStates.foreach(loadAllocatedObject)
    
    val ftxrecovery = transactionRecoveryStates.foldLeft(List[Future[Unit]]()) { (l, trs) => 
      val t = new StoreTransaction(this, trs.txd, trs.localUpdates.getOrElse(Nil))
      
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

  def failedToRead(o: ObjectStoreState, err: ObjectReadError): Unit = synchronized {
    o.loadFailed(err)
  }

  def loadedObjectState(o: ObjectStoreState,
                        ometa: Option[ObjectMetadata],
                        odata: Option[DataBuffer]): Unit = synchronized {
    ometa.foreach(o.metadataLoaded)
    odata.foreach(o.dataLoaded)
  }

  def retainLoadedObject(obj:ObjectStoreState): Unit = loadedObjects += obj.uuid -> obj

  def releaseLoadedObject(obj: ObjectStoreState): Unit = loadedObjects -= obj.uuid
  
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
    val oid = StoreObjectID(ars.newObjectUUID, ars.storePointer)
    val alloc = Some((metadata, ars.objectData))
    val obj = ars.objectType match {
      case ObjectType.Data => new DataObjectStoreState(this, oid, alloc)
      case ObjectType.KeyValue => new KeyValueObjectStoreState(this, oid, alloc)
    }
    obj.incref()
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
            
            fcommitted.onComplete { _ =>
              synchronized {
                mo.decref()
              }
            }

            fcommitted
          } else {
            mo.decref()
            Future.successful(())
          }
        }
        
        Future.sequence(flst).map(_=>())
    }
  }

  def loadObject(pointer: ObjectPointer,
                 loadfn: ObjectStoreState => Future[Either[ObjectReadError, ObjectStoreState]]
                ): ObjectStoreState = synchronized {

    loadedObjects.get(pointer.uuid) match {
      case Some(obj) => // Fast path for already loaded object
        loadfn(obj).map {
          case Left(err) =>
            log.read.info(s"$storeId in-mem load object ${pointer.uuid}. Error $err")

          case Right(_) =>
            log.read.info(s"$storeId in-mem load object ${pointer.uuid}. Rev ${obj.revision} TS ${obj.timestamp} Len ${obj.data.size}")
        }

        obj

      case None => // Not already in memory, load from disk
        pointer.getStorePointer(storeId) match {
          case None =>
            log.read.info(s"$storeId load INVALID object ${pointer.uuid}")
            val obj = pointer.objectType match {
              case ObjectType.Data => new DataObjectStoreState(this, ObjectStoreState.InvalidObjectId, None)
              case ObjectType.KeyValue => new KeyValueObjectStoreState(this, ObjectStoreState.InvalidObjectId, None)
            }
            obj.loadFailed(new InvalidLocalPointer)
            obj

          case Some(storePointer) =>
            val oid = StoreObjectID(pointer.uuid, storePointer)
            val obj = pointer.objectType match {
              case ObjectType.Data => new DataObjectStoreState(this, oid, None)
              case ObjectType.KeyValue => new KeyValueObjectStoreState(this, oid, None)
            }

            // Loads can be slow so we'll incref the object during the load to ensure that multiple reads
            // during the load will find this object in the loadedObjects map
            obj.incref()

            loadfn(obj).foreach { result =>
              result match {
                case Left(err) =>
                  log.read.info(s"$storeId backend load object ${pointer.uuid}. Error $err")

                case Right(_) =>
                  log.read.info(s"$storeId backend load object ${pointer.uuid}. Rev ${obj.revision} TS ${obj.timestamp} Len ${obj.data.size}")
              }

              synchronized {
                obj.decref()
              }
            }

            obj

        }
    }
  }

  override def getObject(pointer: ObjectPointer): Future[Either[ObjectReadError,
                                                                (ObjectMetadata, DataBuffer, List[Lock], Set[UUID])]] = {
    val obj = loadObject(pointer, obj => obj.loadBoth())
    obj.loadBoth().map {
      case Left(err) => Left(err)
      case Right(_) => Right((obj.metadata, obj.data, obj.locks, obj.writeLocks))
    }
  }

  override def getObjectMetadata(pointer: ObjectPointer): Future[Either[ObjectReadError,
                                                                        (ObjectMetadata, List[Lock], Set[UUID])]] = {
    val obj = loadObject(pointer, obj => obj.loadMetadata())
    obj.loadMetadata().map {
      case Left(err) => Left(err)
      case Right(_) => Right((obj.metadata, obj.locks, obj.writeLocks))
    }
  }

  override def getObjectData(pointer: ObjectPointer): Future[Either[ObjectReadError,
                                                                    (DataBuffer, List[Lock], Set[UUID])]] = {
    val obj = loadObject(pointer,obj => obj.loadData())
    obj.loadData().map {
      case Left(err) => Left(err)
      case Right(_) => Right((obj.data, obj.locks, obj.writeLocks))
    }
  }

  private[this] def getStoreTransaction(txd: TransactionDescription, updateData: Option[List[LocalUpdate]]): StoreTransaction = synchronized {
    activeTransactions.getOrElse(txd.transactionUUID, new StoreTransaction(this, txd, updateData.getOrElse(Nil)))
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
}