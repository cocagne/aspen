package com.ibm.aspen.core.rebuilder

import java.util.UUID

import com.ibm.aspen.base.tieredlist.MutableTieredKeyValueList
import com.ibm.aspen.base.{AspenSystem, MissedUpdateIterator, StoragePool}
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.data_store._
import com.ibm.aspen.core.objects.keyvalue.Key
import com.ibm.aspen.core.objects.{DataObjectState, KeyValueObjectState, ObjectPointer, ObjectState}
import com.ibm.aspen.core.read.{FatalReadError, OpportunisticRebuild}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class Rebuilder(val system: AspenSystem, val store: DataStore) extends Logging {

    import store.executionContext
    
    private[this] var ofrepair: Option[Future[(StoragePool, MutableTieredKeyValueList)]] = None
    private[this] var repairing = false
    private[this] var activeRebuilds = Set[UUID]()
    private[this] var pendingRebuilds = Map[UUID, List[() => Unit]]()

    def storeId: DataStoreID = store.storeId

    def pollAndRepairMissedUpdates(system: AspenSystem): Unit = synchronized {

        if (repairing)
            return
        else
            repairing = true

        logger.info(s"$storeId beginning poll of error log for missed updates")

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
            logger.info(s"$storeId completed poll of error log")
            synchronized { repairing = false }
        }
    }

    private def repairMissedUpdates(system: AspenSystem, tree: MutableTieredKeyValueList, iter: MissedUpdateIterator): Future[Unit] = {
        val promise = Promise[Unit]()

        def getObjectPointer(objectUUID: UUID): Future[Option[ObjectPointer]] = for {
            ov <- tree.get(objectUUID)
        } yield ov.map(v => ObjectPointer.fromArray(v.value))

        def next(): Unit = synchronized {
            logger.info(s"$storeId fetching next missed update entry")
            iter.fetchNext() foreach { _ =>
                iter.entry match {
                    case None =>
                        logger.info(s"$storeId missed update entries exhausted")
                        promise.success(())

                    case Some(entry) =>
                        logger.info(s"$storeId found missed update for object ${entry.objectUUID}")

                        val frepair = getObjectPointer(entry.objectUUID).flatMap { optr => repair(system, entry, optr).flatMap(_ => iter.markRepaired()) }

                        frepair onComplete {
                            case Success(_) =>
                                next()

                            case Failure(t) =>
                                logger.error(s"$storeId failed to repair object ${entry.objectUUID}: $t")
                                next() // Could be a transient error.
                        }
                }
            }
        }

        next()

        promise.future
    }

    private def rebuildFinished(objectUUID: UUID): Unit = synchronized {
        // Check to see if another rebuild for this object is pending. If so kick it off
        activeRebuilds -= objectUUID

        pendingRebuilds.get(objectUUID).foreach { lst =>
            val fn = lst.head

            if (lst.tail.isEmpty)
                pendingRebuilds -= objectUUID
            else
                pendingRebuilds += (objectUUID -> lst.tail)

            fn()
        }
    }

    def opportunisticRebuild(message: OpportunisticRebuild): Unit = synchronized {
        /*
        if (!activeRebuilds.contains(message.objectPointer.uuid)) {

            logger.info(s"$storeId opportunistic rebuild beginning for ${message.objectPointer.shortString}")

            activeRebuilds += message.objectPointer.uuid

            val pointer = message.objectPointer
            val objectId = StoreObjectID(pointer.uuid, pointer.getStorePointer(storeId).get)
            val repairUUID = UUID.randomUUID()

            val metadata = ObjectMetadata(message.newRevision, message.newRefcount, message.newTimestamp)
            val data = message.newData

            val pcomplete = Promise[Unit]()

            pcomplete.future onComplete {
                case Failure(err) =>
                    logger.error(s"$storeId opportunistic rebuild FAILED for ${message.objectPointer.shortString}: $err")
                    rebuildFinished(message.objectPointer.uuid)

                case Success(_) =>
                    logger.info(s"$storeId opportunistic rebuild successfully completed for ${message.objectPointer.shortString}")
                    rebuildFinished(message.objectPointer.uuid)
            }

            objectLoader.load(objectId, pointer.objectType, repairUUID).loadBoth().map {
                case Left(_) => synchronized {
                    logger.info(s"$storeId opportunistic rebuild for ${message.objectPointer.shortString}: Storing Missed allocation")
                    backend.putObject(objectId, metadata, data) onComplete {
                        _ => pcomplete.success(())
                    }
                }

                case Right(mo) => synchronized {

                    def complete(): Unit = synchronized {
                        mo.completeOperation(repairUUID)
                        pcomplete.success(())
                    }

                    if (mo.locks.nonEmpty) {
                        logger.info(s"$storeId opportunistic rebuild canceled for ${message.objectPointer.shortString}. Object is locked")
                        complete()
                    }
                    else {
                        logger.info(s"$storeId opportunistic rebuild local state (${mo.revision},${mo.refcount}) rebuild from (${message.oldRevision}, ${message.oldRefcount}) to (${message.newRevision},${message.newRefcount})")

                        val commit = mo match {
                            case d: MutableDataObject =>
                                if(d.revision == message.oldRevision && d.refcount == message.oldRefcount) {
                                    d.restore(metadata, data)
                                    true
                                } else
                                    false

                            case k: MutableKeyValueObject =>
                                if (k.revision == message.oldRevision && k.refcount == message.oldRefcount && k.storeState.allUpdates == message.oldUpdateSet) {
                                    k.restore(metadata, data)
                                    true
                                } else
                                    false
                        }

                        if (commit) {
                            mo.commitBoth() onComplete {
                                _ => complete()
                            }
                        } else
                            complete()
                    }
                }
            }
        }
        */
    }

    private def repair(system: AspenSystem, entry: MissedUpdateIterator.Entry, opointer: Option[ObjectPointer]): Future[Unit] = synchronized {
        val pcomplete = Promise[Unit]()

        if (activeRebuilds.contains(entry.objectUUID)) {
            // Must wait for the current rebuild operation to complete. Queue for execution
            def exec(): Unit = doRepair(system, entry, opointer, pcomplete)
            val plist = exec _ :: pendingRebuilds.getOrElse(entry.objectUUID, Nil)
            pendingRebuilds += (entry.objectUUID -> plist)
        } else
            doRepair(system, entry, opointer, pcomplete)

        pcomplete.future
    }

    private def doRepair(system: AspenSystem, entry: MissedUpdateIterator.Entry, opointer: Option[ObjectPointer], pcomplete: Promise[Unit]): Unit = synchronized {
        /*
        logger.info(s"$storeId repair: beginning repair of missed update for object ${entry.objectUUID}")

        activeRebuilds += entry.objectUUID

        val objectId = StoreObjectID(entry.objectUUID, entry.storePointer)

        val foobj = opointer match {
            case None => Future.successful(None)
            case Some(pointer) =>
                logger.info(s"$storeId repair: reading current state ${pointer.objectType} of object ${entry.objectUUID}")
                system.readObject(pointer, disableOpportunisticRebuild=true).map(Some(_)).recover{ case _: FatalReadError => None }
        }

        val repairUUID = UUID.randomUUID()

        val folocal = opointer match {
            case None => Future.successful(None)
            case Some(pointer) =>
                logger.info(s"$storeId repair: loading local state of object ${entry.objectUUID}")

                objectLoader.load(objectId, pointer.objectType, repairUUID).loadBoth().map {
                    case Left(err) =>
                        logger.info(s"$storeId repair: failed to get state of object ${entry.objectUUID}. Error: $err")
                        None
                    case Right(obj) =>
                        synchronized {
                            logger.info(s"$storeId repair: got local state of object ${entry.objectUUID}. Rev ${obj.revision} Ref ${obj.refcount}")
                        }
                        Some(obj)
                }
        }

        def isAllocated: Future[Boolean] = {
            val key = Key(entry.objectUUID)
            for {
                pool <- system.getStoragePool(storeId.poolUUID)
                tree <- pool.getAllocationTree(system.retryStrategy)
                node <- tree.fetchMutableNode(key)
            } yield node.kvos.contents.contains(key)
        }

        def getLocalData(os: ObjectState): DataBuffer = {
            os.getRebuildDataForStore(storeId) match {
                case None =>
                    logger.error(s"$storeId repair: got local state of object ${entry.objectUUID}. Attempted to rebuild pointer on non-hosting store")
                    throw new Exception("Attempted rebuild on non hosting store")

                case Some(data) => data
            }
        }

        def fix(oobj: Option[ObjectState], olocal: Option[MutableObject]): Future[Unit] = (oobj, olocal) match {

            case (None, None) => isAllocated map { allocated =>
                if (allocated) {
                    logger.info(s"$storeId repair: Object is in an indeterminate state. Cannot yet repair object ${entry.objectUUID}")
                    throw new Exception("Object is in an indeterminate state. Cannot repair yet")
                }
                else {
                    // Delete local object state (if we have any)
                    logger.info(s"$storeId repair: Reparing missed delete for object ${entry.objectUUID}")
                    synchronized { backend.deleteObject(objectId) }
                }
            }

            case (None, Some(mo)) =>
                isAllocated map { allocated => synchronized {
                    if (allocated) {
                        logger.info(s"$storeId repair: Object is not fully deleted. Cannot repair object ${entry.objectUUID}")
                        throw new Exception("Object not fully deleted. Cannot repair yet")
                    }
                    else if (mo.locks.nonEmpty) {
                        logger.info(s"$storeId repair: Object is locked. Cannot repair object ${entry.objectUUID}")
                        throw new Exception("Object locked. Cannot repair yet")
                    }
                    else {
                        logger.info(s"$storeId repair: Repairing missed delete for object ${entry.objectUUID}")
                        synchronized { backend.deleteObject(objectId) }
                    }
                }}

            case (Some(os), None) => synchronized {
                val metadata = ObjectMetadata(os.revision, os.refcount, os.timestamp)
                val data = getLocalData(os)
                logger.info(s"$storeId repair: Repairing missed allocation for object ${entry.objectUUID}")
                backend.putObject(objectId, metadata, data)
            }

            case (Some(kvos: KeyValueObjectState), Some(mo: MutableKeyValueObject)) => synchronized {
                if (kvos.timestamp > mo.timestamp || kvos.lastUpdateTimestamp > mo.storeState.lastUpdateTimestamp) {
                    logger.info(s"$storeId repair: Repairing missed data update for object ${entry.objectUUID}.")
                    mo.restore(ObjectMetadata(kvos.revision, kvos.refcount, kvos.timestamp), getLocalData(kvos))
                    mo.commitBoth()
                } else if (kvos.refcount != mo.refcount) {
                    logger.info(s"$storeId repair: Repairing missed refcount update for object ${entry.objectUUID}.")
                    mo.setMetadata(ObjectMetadata(mo.revision, kvos.refcount, mo.timestamp))
                    mo.commitMetadata()
                } else
                    Future.unit
            }

            case (Some(dos: DataObjectState), Some(mo: MutableDataObject)) => synchronized {
                if (dos.lastUpdateTimestamp > mo.timestamp) {
                    logger.info(s"$storeId repair: Repairing missed data update for object ${entry.objectUUID}.")
                    mo.restore(ObjectMetadata(dos.revision, dos.refcount, dos.timestamp), getLocalData(dos))
                    mo.commitBoth()
                } else if (dos.refcount != mo.refcount) {
                    logger.info(s"$storeId repair: Repairing missed refcount update for object ${entry.objectUUID}.")
                    mo.setMetadata(ObjectMetadata(mo.revision, dos.refcount, mo.timestamp))
                    mo.commitMetadata()
                } else
                    Future.unit

            }

            case _ =>
                logger.error(s"$storeId repair: Invalid Repair State for object ${entry.objectUUID}.")
                Future.unit
        }

        val fcomplete = for {
            oobj <- foobj
            olocal <- folocal
            _ <- fix(oobj, olocal)
        } yield {
            olocal.foreach(mo => synchronized { mo.completeOperation(repairUUID) })
        }

        pcomplete.completeWith(fcomplete)

        pcomplete.future.onComplete {
            case Failure(err) =>
                logger.warn(s"$storeId repair: FAILD repair actions for object ${entry.objectUUID}: $err")
                rebuildFinished(entry.objectUUID)

            case Success(_) =>
                logger.info(s"$storeId repair: Completed repairs for ${entry.objectUUID}")
                rebuildFinished(entry.objectUUID)
        }
        */
    }
}
