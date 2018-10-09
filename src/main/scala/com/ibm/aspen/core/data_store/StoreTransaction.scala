package com.ibm.aspen.core.data_store

import java.util.UUID

import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}
import com.ibm.aspen.core.objects.{ObjectPointer, ObjectRevision}
import com.ibm.aspen.core.transaction._
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{Future, Promise}

object StoreTransaction {

  object LoadType extends Enumeration {
    val MetadataOnly: LoadType.Value = Value("MetadataOnly")
    val DataOnly: LoadType.Value = Value("DataOnly")
    val Both: LoadType.Value = Value("Both")
  }

  private class Ohelper(val pointer: ObjectPointer) {
    var loadData: Boolean = false
    var loadMeta: Boolean = false
    def loadType: LoadType.Value = if (loadData && loadMeta)
      LoadType.Both
    else if(loadData)
      LoadType.DataOnly
    else
      LoadType.MetadataOnly
  }

  def getObjectLoadRequirements(requirements: List[TransactionRequirement]): List[(ObjectPointer, LoadType.Value)] = {
    var helpers = Map[ObjectPointer, Ohelper]()

    def getHelper(req: TransactionRequirement): Ohelper = helpers.getOrElse(req.objectPointer, new Ohelper(req.objectPointer))

    def needMeta(req: TransactionRequirement): Unit = {
      val helper = getHelper(req)
      helper.loadMeta = true
      helpers += (req.objectPointer -> helper)
    }

    def needBoth(req: TransactionRequirement): Unit = {
      val helper = getHelper(req)
      helper.loadMeta = true
      helper.loadData = true
      helpers += (req.objectPointer -> helper)
    }

    requirements.foreach {
      case req: DataUpdate => req.operation match {
        case DataUpdateOperation.Overwrite => needMeta(req)
        case DataUpdateOperation.Append    => needBoth(req)
      }
      case req: RefcountUpdate => needMeta(req)
      case req: VersionBump => needMeta(req)
      case req: RevisionLock => needMeta(req)
      case req: KeyValueUpdate =>
        val helper = getHelper(req)
        req.requiredRevision.foreach(_ => helper.loadMeta = true)
        helper.loadData = true
        helpers += (req.objectPointer -> helper)
    }

    helpers.toList.map(t => t._1 -> t._2.loadType)
  }
}

class StoreTransaction(val store: DataStoreFrontend, 
                       val txd: TransactionDescription, 
                       updateData: List[LocalUpdate]) extends Logging {

  import StoreTransaction._
  import store.executionContext

  def storeId: DataStoreID = store.storeId

  logger.info(s"$storeId tx: ${txd.transactionUUID} Beginning transaction")

  store.activeTransactions += txd.transactionUUID -> this

  var locked = false
  var committed = false

  // Filter Objects & Transaction Requirements down to just the set of objects hosted by this store

  val localObjects: Set[ObjectPointer] = txd.allReferencedObjectsSet.filter { ptr =>
    ptr.poolUUID == storeId.poolUUID && ptr.storePointers.exists(_.poolIndex == storeId.poolIndex)
  }

  val requirements: List[TransactionRequirement] = txd.requirements.filter(r => localObjects.contains(r.objectPointer))

  val dataUpdates: Map[UUID, DataBuffer] = updateData.map(lu => lu.objectUUID -> lu.data).toMap

  private var lockRequests: List[Promise[List[ObjectTransactionError]]] = Nil

  val (objects: Map[UUID, ObjectStoreState],
       objectsLoaded: Future[Unit]) = {

    val temp = getObjectLoadRequirements(requirements).map { t =>
      val (ptr, loadType) = t
      val (obj, loadFuture) = loadType match {
        case LoadType.MetadataOnly =>
          val o = store.loadObject(ptr, obj => obj.loadMetadata())
          (o, o.loadMetadata())

        case LoadType.DataOnly =>
          val o = store.loadObject(ptr, obj => obj.loadData())
          (o, o.loadData())

        case LoadType.Both =>
          val o = store.loadObject(ptr, obj => obj.loadBoth())
          (o, o.loadBoth())
      }

      obj.incref() // Ensure all objects referenced by the transaction remain loaded until the transaction is discarded

      ptr.uuid -> (obj, loadFuture)
    }

    (temp.map(t => t._1 -> t._2._1).toMap,
      Future.sequence(temp.map(t => t._2._2)).map(_=>()))
  }

  protected[data_store] def lockObjects(): Unit = if (!committed && !locked) {
    logger.info(s"$storeId tx: ${txd.transactionUUID} Locking to transaction")

    locked = true
    store.lockedTransactions += (txd.transactionUUID -> this)

    requirements foreach { r =>

      val obj = objects(r.objectPointer.uuid)

      r match {
        case _: DataUpdate     => obj.objectRevisionWriteLock = Some(txd)
        case _: RefcountUpdate => obj.objectRefcountWriteLock = Some(txd)
        case _: VersionBump    => obj.objectRevisionWriteLock = Some(txd)
        case _: RevisionLock   => obj.objectRevisionReadLocks += (txd.transactionUUID -> txd)
        case kv: KeyValueUpdate =>
          val kvobj = obj.asInstanceOf[KeyValueObjectStoreState]

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

  private def unblockDelayedTransactions(): Unit = {
    // Attempt to lock any transactions that were dependent upon this one completing
    store.delayedTransactions.get(txd.transactionUUID).foreach { delayedSet =>
      store.delayedTransactions -= txd.transactionUUID
      delayedSet.foreach( st => st.checkRequirementsAndLock(None) )
    }
  }

  protected[data_store] def discard(): Unit = {
    logger.info(s"$storeId tx: ${txd.transactionUUID} Discarding transaction")

    releaseObjects()

    store.activeTransactions -= txd.transactionUUID

    unblockDelayedTransactions()

    objects.valuesIterator.foreach(_.decref())
  }

  protected[data_store] def releaseObjects(): Unit = if (locked) {
    logger.info(s"$storeId tx: ${txd.transactionUUID} Unlocking from transaction")

    locked = false

    store.lockedTransactions -= txd.transactionUUID

    requirements foreach { r =>

      val obj = objects(r.objectPointer.uuid)

      r match {
        case _: DataUpdate     => obj.objectRevisionWriteLock = None

        case _: RefcountUpdate => obj.objectRefcountWriteLock = None

        case _: VersionBump    => obj.objectRevisionWriteLock = None

        case _: RevisionLock => obj.objectRevisionReadLocks -= txd.transactionUUID

        case kv: KeyValueUpdate =>
          val kvobj = obj.asInstanceOf[KeyValueObjectStoreState]

          kv.requiredRevision match {
            case Some(_) => obj.objectRevisionWriteLock = None

            case None =>
              kv.requirements.foreach { req =>
                kvobj.keyRevisionWriteLocks -= req.key
              }
          }
      }
    }

    unblockDelayedTransactions()
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

        if (logger.delegate.isWarnEnabled()) {
          logger.warn(s"$storeId tx: ${txd.transactionUUID} not locking due to errors:")
          errors.foreach( err => logger.warn(s"$storeId tx: ${txd.transactionUUID} ERROR: $err") )
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
            val dset = store.delayedTransactions.getOrElse(lockedTxUUID, Set()) + this

            waitingForTransactions += lockedTxUUID
            store.delayedTransactions += (lockedTxUUID -> dset)
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
        logger.info(s"$storeId tx: ${txd.transactionUUID} delaying action until transactions complete: $waitingForTransactions")
        op.foreach { p =>
          lockRequests = p :: lockRequests
        }
      }
    }
  }



  def getRequirementErrors(requirement: TransactionRequirement): List[ObjectTransactionError] = {
    var errors = List[ObjectTransactionError]()

    def err(e: ObjectTransactionError): Unit = errors = e :: errors

    import scala.language.implicitConversions

    implicit def mo2ptr(mo: ObjectStoreState): ObjectPointer = txd.allReferencedObjectsSet.find(ptr => ptr.uuid == mo.objectId.objectUUID).get


    val pointer = requirement.objectPointer
    val obj = objects(pointer.uuid)

    obj.loadError match {
      case Some(readErr) => err(TransactionReadError(pointer, readErr))

      case None => requirement match {
        case du: DataUpdate =>
          obj.getTransactionPreventingRevisionWriteLock(txd) foreach { lockedTxd => err(TransactionCollision(pointer, lockedTxd)) }

          if (obj.revision != du.requiredRevision)
            err(RevisionMismatch(pointer, du.requiredRevision, obj.revision))

          if (obj.timestamp > HLCTimestamp(txd.startTimestamp))
            err(TransactionTimestampError(pointer))

          dataUpdates.get(obj.objectId.objectUUID) match {
            case None => err(MissingUpdateContent(pointer))

            case Some(data) =>
              val haveSpace = du.operation match {
                case DataUpdateOperation.Overwrite => store.backend.haveFreeSpaceForOverwrite(obj.objectId, obj.data.size, data.size)
                case DataUpdateOperation.Append    => store.backend.haveFreeSpaceForAppend(obj.objectId, obj.data.size, obj.data.size + data.size)
              }
              if (!haveSpace)
                err(InsufficientFreeSpace(pointer))
          }


        case ru: RefcountUpdate =>
          obj.getTransactionPreventingRefcountWriteLock(txd) foreach { lockedTxd => err(TransactionCollision(pointer, lockedTxd)) }

          if (obj.refcount != ru.requiredRefcount)
            err(RefcountMismatch(pointer, ru.requiredRefcount, obj.refcount))


        case vb: VersionBump =>
          obj.getTransactionPreventingRevisionWriteLock(txd) foreach { lockedTxd => err(TransactionCollision(pointer, lockedTxd)) }

          if (obj.revision != vb.requiredRevision)
            err(RevisionMismatch(pointer, vb.requiredRevision, obj.revision))

        case rl: RevisionLock =>
          obj.getTransactionPreventingRevisionReadLock(txd) foreach { lockedTxd => err(TransactionCollision(pointer, lockedTxd)) }

          if (obj.revision != rl.requiredRevision)
            err(RevisionMismatch(pointer, rl.requiredRevision, obj.revision))

        case kv: KeyValueUpdate =>
          obj match {
            case kvobj: KeyValueObjectStoreState =>

              kv.requiredRevision.foreach { requiredRevision =>
                obj.getTransactionPreventingRevisionWriteLock(txd) foreach { lockedTxd => err(TransactionCollision(pointer, lockedTxd)) }

                if (obj.revision != requiredRevision)
                  err(RevisionMismatch(pointer, requiredRevision, obj.revision))

                if (obj.timestamp > HLCTimestamp(txd.startTimestamp))
                  err(TransactionTimestampError(pointer))
              }

              dataUpdates.get(obj.objectId.objectUUID) match {
                case None => err(MissingUpdateContent(pointer))

                case Some(data) =>
                  val haveSpace = kv.updateType match {
                    case KeyValueUpdate.UpdateType.Update =>

                      val meetsSizeRequirement = requirement.objectPointer.size match {
                        case None => true
                        case Some(maxSize) =>
                          val kvoss = obj.asInstanceOf[KeyValueObjectStoreState].kvcontent

                          val updatedKvoss = kvoss.update(data, ObjectRevision(txd.transactionUUID), HLCTimestamp(txd.startTimestamp))

                          updatedKvoss.encodedSize <= maxSize
                      }
                      meetsSizeRequirement && store.backend.haveFreeSpaceForAppend(obj.objectId, obj.data.size, obj.data.size + data.size)
                  }

                  if (!haveSpace)
                    err(InsufficientFreeSpace(pointer))
              }

              if (kv.requirements.nonEmpty) {

                val kvoss = kvobj.kvcontent

                val objectLocked = kvobj.objectRevisionWriteLock match {
                  case None => false
                  case Some(lockedTxd) => lockedTxd.transactionUUID != txd.transactionUUID
                }

                if (objectLocked) {
                  err(TransactionCollision(pointer, kvobj.objectRevisionWriteLock.get))
                } else {
                  kv.requirements.foreach { req =>
                    val ov = kvoss.idaEncodedContents.get(req.key)

                    ov.foreach { v =>
                      if (v.timestamp > HLCTimestamp(txd.startTimestamp))
                        err(TransactionTimestampError(pointer))
                    }

                    kvobj.keyRevisionWriteLocks.get(req.key) foreach { lockedTxd =>
                      if (lockedTxd.transactionUUID != txd.transactionUUID)
                        err(KeyValueRequirementError(pointer, req.key))
                    }

                    req.tsRequirement match {
                      case KeyValueUpdate.TimestampRequirement.Equals => ov match {
                        case None => err(KeyValueRequirementError(pointer, req.key))
                        case Some(v) => if (v.timestamp != req.timestamp) err(KeyValueRequirementError(pointer, req.key))
                      }
                      case KeyValueUpdate.TimestampRequirement.LessThan => ov match {
                        case None => err(KeyValueRequirementError(pointer, req.key))
                        case Some(v) => if (req.timestamp.asLong >= v.timestamp.asLong) err(KeyValueRequirementError(pointer, req.key))
                      }
                      case KeyValueUpdate.TimestampRequirement.Exists => ov match {
                        case None => err(KeyValueRequirementError(pointer, req.key))
                        case Some(_) =>
                      }
                      case KeyValueUpdate.TimestampRequirement.DoesNotExist => ov match {
                        case None =>
                        case Some(_) => err(KeyValueRequirementError(pointer, req.key))
                      }
                    }
                  }
                }
              }

            case _ => err(InvalidObjectType(pointer))
          }

      }
    }

    errors
  }


  def commit(): Future[Unit] = if (committed)
    Future.unit
  else {

    logger.info(s"$storeId tx: ${txd.transactionUUID} Committing")
    committed = true

    val timestamp = HLCTimestamp(txd.startTimestamp)

    class CommitState(val obj: ObjectStoreState) {
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
      if (cs.obj.loadError.isEmpty) {

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
                  case d: DataObjectStoreState => d.overwriteData(data)
                  case _: KeyValueObjectStoreState => logger.error(s"$storeId tx: ${txd.transactionUUID} Invalid Overwrite on key-value object ${requirement.objectPointer.uuid}")
                }
                case DataUpdateOperation.Append => cs.obj match {
                  case d: DataObjectStoreState => d.appendData(data)
                  case _: KeyValueObjectStoreState => logger.error(s"$storeId tx: ${txd.transactionUUID} Invalid Append on key-value object ${requirement.objectPointer.uuid}")
                }
              }
              cs.obj.metadata = cs.obj.metadata.copy(revision=ObjectRevision(txd.transactionUUID), timestamp=timestamp)
              cs.commitData = true
              cs.commitMetadata = true
              logger.info(s"$storeId tx: ${txd.transactionUUID} Committing DataUpdate ${du.operation} for object ${requirement.objectPointer.uuid}")

            case ru: RefcountUpdate =>
              // TODO - Do we want to update the timestamp on refcount changes?
              cs.obj.metadata = cs.obj.metadata.copy(refcount=ru.newRefcount, timestamp=timestamp)
              cs.commitMetadata = true
              if (cs.obj.refcount.count == 0) {
                cs.deleteObject = true
                logger.info(s"$storeId tx: ${txd.transactionUUID} Committing RefcountUpdate to DELETE object ${requirement.objectPointer.uuid}")
              } else {
                logger.info(s"$storeId tx: ${txd.transactionUUID} Committing RefcountUpdate to ${ru.newRefcount} for object ${requirement.objectPointer.uuid}")
              }

            case _: VersionBump =>
              cs.obj.metadata = cs.obj.metadata.copy(revision=ObjectRevision(txd.transactionUUID), timestamp=timestamp)
              cs.commitMetadata = true
              logger.info(s"$storeId tx: ${txd.transactionUUID} Committing VersionBump for object ${requirement.objectPointer.uuid}")

            case _: RevisionLock => // Nothing to do

            case kv: KeyValueUpdate =>
              val kvobj = cs.obj.asInstanceOf[KeyValueObjectStoreState]

              kv.requiredRevision.foreach { _ =>
                cs.obj.metadata = cs.obj.metadata.copy(revision=ObjectRevision(txd.transactionUUID), timestamp=timestamp)
                cs.commitMetadata = true
              }

              logger.info(s"$storeId tx: ${txd.transactionUUID} Committing KeyValue ${kv.updateType} for object ${requirement.objectPointer.uuid}")

              kv.updateType match {
                case KeyValueUpdate.UpdateType.Update =>
                  cs.commitData = true
                  val data = dataUpdates(requirement.objectPointer.uuid)
                  kvobj.update(data, ObjectRevision(txd.transactionUUID), timestamp)
              }
          }
        } else {
          if (logger.delegate.isWarnEnabled()) {
            logger.warn(s"$storeId tx: ${txd.transactionUUID} SKIPPING commit for operation ${requirement.getClass.getSimpleName} on object ${requirement.objectPointer.uuid} due to errors:")
            requirementErrors.foreach( err => logger.warn(s"$storeId tx: ${txd.transactionUUID} ERROR: $err") )
          }
        }
      }
    }

    Future.sequence { csmap.valuesIterator.map { cs =>
      if (cs.deleteObject) {
        cs.obj.deleted = true
        store.backend.deleteObject(cs.obj.objectId)
      } else {
        if (cs.commitData && cs.commitMetadata)
          cs.obj.commitBoth()

        else if (cs.commitMetadata)
          cs.obj.commitMetadata()

        else if (cs.commitData)
          cs.obj.commitData()

        else
          Future.successful(())
      }
    }}.map(_ => ())
  }
}
