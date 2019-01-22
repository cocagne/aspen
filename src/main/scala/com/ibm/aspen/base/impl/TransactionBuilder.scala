package com.ibm.aspen.base.impl

import java.util.UUID

import com.ibm.aspen.base.{ConflictingRequirements, MultipleDataUpdatesToObject, MultipleRefcountUpdatesToObject, OpportunisticRebuildManager}
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.objects.{KeyValueObjectPointer, ObjectPointer, ObjectRefcount, ObjectRevision}
import com.ibm.aspen.core.transaction._
import com.ibm.aspen.core.{DataBuffer, HLCTimestamp}

object TransactionBuilder {
  case class KVUpdate(
      pointer: KeyValueObjectPointer,
      updateType: KeyValueUpdate.UpdateType.Value,
      requiredRevision: Option[ObjectRevision],
      requirements: List[KeyValueUpdate.KVRequirement],
      operations: List[KeyValueOperation])
}

class TransactionBuilder(
    val transactionUUID: UUID,
    val chooseDesignatedLeader: ObjectPointer => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
    val clientId: ClientID) {
  
  import TransactionBuilder._
  
  private [this] var requirements = List[TransactionRequirement]()
  
  private [this] var refcountUpdates = Set[ObjectPointer]()
  private [this] var updatingObjects = Set[ObjectPointer]() // Tracks UUIDs of all objects being modified 
  private [this] var revisionLocks = Set[ObjectPointer]() // UUIDs of all revision-locked objects
  private [this] var dataObjectUpdates = Map[ObjectPointer, DataBuffer]()
  private [this] var keyValueUpdates = Map[KeyValueObjectPointer, KVUpdate]()
  
  private [this] var finalizationActions = List[SerializedFinalizationAction]()
  private [this] var notifyOnResolution = Set[DataStoreID]()
  private [this] var notes = List[String]()
  private [this] var addMissedUpdateTrackingFA = true
  private [this] var missedCommitDelayInMs = 1000
  private [this] val minimumTimestamp = HLCTimestamp.now

  def buildTranaction(opportunisticRebuildManager: OpportunisticRebuildManager, transactionUUID: UUID): (TransactionDescription,
    Map[DataStoreID, (List[LocalUpdate], List[PreTransactionOpportunisticRebuild])], HLCTimestamp) = synchronized {

    HLCTimestamp.update(minimumTimestamp)

    val startTimestamp = HLCTimestamp.now

    keyValueUpdates.valuesIterator.foreach { kvu =>
      requirements = KeyValueUpdate(kvu.pointer, kvu.updateType, kvu.requiredRevision, kvu.requirements, startTimestamp) :: requirements
    }

    // Ensure the transaction has at least one requirement
    require(requirements.nonEmpty)

    val primaryObject = requirements.map(_.objectPointer).maxBy(ptr => ptr.ida)
    val designatedLeaderUID = chooseDesignatedLeader(primaryObject)
    val originatingClient = Some(clientId)

    if (addMissedUpdateTrackingFA)
      finalizationActions = MissedUpdateFinalizationAction.createSerializedFA(missedCommitDelayInMs) :: finalizationActions

    val txd = TransactionDescription(transactionUUID, startTimestamp.asLong, primaryObject, designatedLeaderUID,
                                     requirements, finalizationActions, originatingClient, notifyOnResolution.toList,
                                     notes)

    var updates = Map[DataStoreID, (List[LocalUpdate], List[PreTransactionOpportunisticRebuild])]()

    def addUpdate(pointer: ObjectPointer, encoded: Array[DataBuffer]): Unit = {
      val mpr = opportunisticRebuildManager.getPreTransactionOpportunisticRebuild(pointer)

      pointer.storePointers zip encoded foreach { x =>
        val (sp, bb) = x
        val storeId = DataStoreID(pointer.poolUUID, sp.poolIndex)
        val lu = LocalUpdate(pointer.uuid, bb)

        val opr = mpr.get(sp.poolIndex)

        updates.get(storeId) match {
          case None =>
            val lpr = opr match {
              case None => List()
              case Some(pr) => pr :: Nil
            }
            updates += (storeId -> (List(lu), lpr))

          case Some(t) =>
            val newLu = lu :: t._1
            val newLp = opr match {
              case None => t._2
              case Some(pr) => pr :: t._2
            }
            updates += (storeId -> (newLu, newLp))
        }
      }
    }

    dataObjectUpdates foreach { t =>
      val (objectPointer, buf) = t

      addUpdate(objectPointer, objectPointer.ida.encode(buf))
    }

    keyValueUpdates.valuesIterator.foreach { kvu =>
      addUpdate(kvu.pointer, KeyValueOperation.encode(kvu.operations, kvu.pointer.ida))
    }

    (txd, updates, startTimestamp)
  }

  def disableMissedUpdateTracking(): Unit = synchronized {
    addMissedUpdateTrackingFA = false
  }

  def setMissedCommitDelayInMs(msec: Int): Unit = synchronized {
    missedCommitDelayInMs = msec
  }

  def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = synchronized {
    //println(s"   TXB Append txid $transactionUUID object ${objectPointer.uuid}")
    if (updatingObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    if (revisionLocks.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)

    updatingObjects += objectPointer
    dataObjectUpdates += (objectPointer -> data)
    requirements = DataUpdate(objectPointer, requiredRevision, DataUpdateOperation.Append) :: requirements

    ObjectRevision(transactionUUID)
  }

  def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = synchronized {
    //println(s"   TXB Overwrite txid $transactionUUID object ${objectPointer.uuid}")
    if (updatingObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    if (revisionLocks.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)

    updatingObjects += objectPointer
    dataObjectUpdates += (objectPointer -> data)
    requirements  = DataUpdate(objectPointer, requiredRevision, DataUpdateOperation.Overwrite) :: requirements

    ObjectRevision(transactionUUID)
  }

  def update(
      pointer: KeyValueObjectPointer,
      requiredRevision: Option[ObjectRevision],
      requirements: List[KeyValueUpdate.KVRequirement],
      operations: List[KeyValueOperation]): Unit = synchronized {
    //println(s"   TXB KV Append txid $transactionUUID object ${pointer.uuid}")
    if (updatingObjects.contains(pointer))
      throw MultipleDataUpdatesToObject(pointer)
    if (revisionLocks.contains(pointer))
      throw ConflictingRequirements(pointer)

    keyValueUpdates += (pointer -> KVUpdate(pointer, KeyValueUpdate.UpdateType.Update, requiredRevision, requirements, operations))
  }

  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount = synchronized {
    //println(s"   TXB SetRef txid $transactionUUID object ${objectPointer.uuid}")
    if (refcountUpdates.contains(objectPointer))
      throw MultipleRefcountUpdatesToObject(objectPointer)

    refcountUpdates += objectPointer
    requirements  = RefcountUpdate(objectPointer, requiredRefcount, refcount) :: requirements

    if (refcount.count == 0)
      finalizationActions = DeleteFinalizationAction.createSerializedFA(objectPointer) :: finalizationActions

    refcount
  }

  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): ObjectRevision = synchronized {
    //println(s"   TXB BumpVersion txid $transactionUUID object ${objectPointer.uuid}")
    if (updatingObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    if (revisionLocks.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)

    updatingObjects += objectPointer
    requirements = VersionBump(objectPointer, requiredRevision) :: requirements

    ObjectRevision(transactionUUID)
  }

  def lockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit = synchronized {
    //println(s"   TXB LockRevision txid $transactionUUID object ${objectPointer.uuid}")
    if (updatingObjects.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)

    revisionLocks += objectPointer
    requirements = RevisionLock(objectPointer, requiredRevision) :: requirements
  }

  def ensureHappensAfter(timestamp: HLCTimestamp): Unit = HLCTimestamp.update(timestamp)

  def timestamp(): HLCTimestamp = synchronized { minimumTimestamp }
  
  def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit = synchronized {
    finalizationActions = SerializedFinalizationAction(finalizationActionUUID, serializedContent) :: finalizationActions
  }
  
  def addFinalizationAction(finalizationActionUUID: UUID): Unit = synchronized {
    finalizationActions = SerializedFinalizationAction(finalizationActionUUID, new Array[Byte](0)) :: finalizationActions
  }
  
  def addNotifyOnResolution(storesToNotify: Set[DataStoreID]): Unit = synchronized {
    notifyOnResolution = notifyOnResolution ++ storesToNotify
  }

  def note(note: String): Unit = synchronized {
    notes = note :: notes
  }
}