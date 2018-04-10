package com.ibm.aspen.base.impl

import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import java.nio.ByteBuffer
import com.ibm.aspen.core.objects.ObjectRefcount
import java.util.UUID
import com.ibm.aspen.core.network.ClientID
import com.ibm.aspen.core.transaction.DataUpdate
import com.ibm.aspen.core.transaction.RefcountUpdate
import com.ibm.aspen.core.transaction.SerializedFinalizationAction
import com.ibm.aspen.base.MultipleDataUpdatesToObject
import com.ibm.aspen.core.transaction.DataUpdateOperation
import com.ibm.aspen.base.MultipleRefcountUpdatesToObject
import com.ibm.aspen.core.transaction.TransactionDescription
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.LocalUpdate
import com.ibm.aspen.core.transaction.TransactionRequirement
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.transaction.VersionBump
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.KeyValueObjectPointer
import com.ibm.aspen.core.objects.keyvalue.KeyValueOperation
import com.ibm.aspen.core.transaction.KeyValueUpdate
import com.ibm.aspen.core.objects.keyvalue.KeyValueObjectCodec
import com.ibm.aspen.base.ConflictingRequirements
import com.ibm.aspen.core.transaction.RevisionLock

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
    val chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
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
  private [this] var happensAfter: Option[HLCTimestamp] = None
  
  def buildTranaction(transactionUUID: UUID): (TransactionDescription, Map[DataStoreID, List[LocalUpdate]]) = synchronized {
    
    val startTimestamp = happensAfter match {
      case Some(ts) => HLCTimestamp.happensAfter(ts)
      case None => HLCTimestamp.now
    }
    
    keyValueUpdates.valuesIterator.foreach { kvu =>
      requirements = KeyValueUpdate(kvu.pointer, kvu.updateType, kvu.requiredRevision, kvu.requirements, startTimestamp) :: requirements
    }
    
    // Ensure the transaction has at least one requirement
    require(!requirements.isEmpty)
    
    val primaryObject = requirements.map(_.objectPointer).maxBy(ptr => ptr.ida)
    val designatedLeaderUID = chooseDesignatedLeader(primaryObject)
    val originatingClient = Some(clientId)
    
    val txd = TransactionDescription(transactionUUID, startTimestamp.asLong, primaryObject, designatedLeaderUID, 
                                     requirements, finalizationActions, originatingClient, notifyOnResolution.toList)
           
    var updates = Map[DataStoreID, List[LocalUpdate]]()
    
    def addUpdate(pointer: ObjectPointer, encoded: Array[DataBuffer]): Unit = {
      pointer.storePointers zip encoded foreach { x =>
        val (sp, bb) = x
        val storeId = DataStoreID(pointer.poolUUID, sp.poolIndex)
        val lu = LocalUpdate(pointer.uuid, bb)
        
        updates.get(storeId) match {
          case None => updates += (storeId -> List(lu))
          case Some(lst) => 
            val newList = lu :: lst
            updates += (storeId -> newList)
        }
      }
    }
    
    dataObjectUpdates foreach { t => 
      val (objectPointer, buf) = t
      
      addUpdate(objectPointer, objectPointer.ida.encode(buf)) 
    }
    
    keyValueUpdates.valuesIterator.foreach { kvu => 
      addUpdate(kvu.pointer, KeyValueObjectCodec.encodeUpdate(kvu.pointer.ida, kvu.operations)) 
    }
                                     
    (txd, updates)
  }
  
  def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = synchronized {
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
    if (updatingObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    if (revisionLocks.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)
    
    updatingObjects += objectPointer
    dataObjectUpdates += (objectPointer -> data)
    requirements  = DataUpdate(objectPointer, requiredRevision, DataUpdateOperation.Overwrite) :: requirements
    
    ObjectRevision(transactionUUID)
  }
  
  def append(
      pointer: KeyValueObjectPointer, 
      requiredRevision: Option[ObjectRevision],
      requirements: List[KeyValueUpdate.KVRequirement],
      operations: List[KeyValueOperation]): Unit = synchronized {
        
    if (updatingObjects.contains(pointer))
      throw MultipleDataUpdatesToObject(pointer)
    if (revisionLocks.contains(pointer))
      throw ConflictingRequirements(pointer)
    
    keyValueUpdates += (pointer -> KVUpdate(pointer, KeyValueUpdate.UpdateType.Append, requiredRevision, requirements, operations))
  }
  
  def overwrite(
      pointer: KeyValueObjectPointer, 
      requiredRevision: ObjectRevision,
      requirements: List[KeyValueUpdate.KVRequirement],
      operations: List[KeyValueOperation]): Unit = synchronized {
        
    if (updatingObjects.contains(pointer))
      throw MultipleDataUpdatesToObject(pointer)
    if (revisionLocks.contains(pointer))
      throw ConflictingRequirements(pointer)
    
    keyValueUpdates += (pointer -> KVUpdate(pointer, KeyValueUpdate.UpdateType.Overwrite, Some(requiredRevision), requirements, operations))
  }
  
  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount = synchronized {
    if (refcountUpdates.contains(objectPointer))
      throw MultipleRefcountUpdatesToObject(objectPointer)
    
    refcountUpdates += objectPointer
    requirements  = RefcountUpdate(objectPointer, requiredRefcount, refcount) :: requirements
    
    refcount
  }
  
  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): ObjectRevision = synchronized {
    if (updatingObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    if (revisionLocks.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)
    
    updatingObjects += objectPointer
    requirements = VersionBump(objectPointer, requiredRevision) :: requirements
    
    ObjectRevision(transactionUUID)
  }
  
  def lockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit = synchronized {
    if (updatingObjects.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)
    
    revisionLocks += objectPointer
    requirements = RevisionLock(objectPointer, requiredRevision) :: requirements
  } 
  
  def ensureHappensAfter(timestamp: HLCTimestamp): Unit = synchronized {
    happensAfter match {
      case Some(ts) => if (timestamp.compareTo(ts) > 0) happensAfter = Some(timestamp)
      case None => happensAfter = Some(timestamp)
    }
  }
  
  def timestamp(): HLCTimestamp = synchronized {
    happensAfter match {
      case Some(ts) => HLCTimestamp.happensAfter(ts)
      case None => HLCTimestamp.now
    }
  }
  
  def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit = synchronized {
    finalizationActions = SerializedFinalizationAction(finalizationActionUUID, serializedContent) :: finalizationActions
  }
  
  def addNotifyOnResolution(storesToNotify: Set[DataStoreID]): Unit = synchronized {
    notifyOnResolution = notifyOnResolution ++ storesToNotify
  }
}