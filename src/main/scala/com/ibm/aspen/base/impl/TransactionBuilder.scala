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

class TransactionBuilder(
    chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
    clientId: ClientID) {
  
  private [this] var requirements = List[TransactionRequirement]()
  
  private [this] var refcountUpdates = Set[ObjectPointer]()
  private [this] var objectUpdates = Map[ObjectPointer, DataBuffer]()
  
  private [this] var finalizationActions = List[SerializedFinalizationAction]()
  private [this] var notifyOnResolution = Set[DataStoreID]()
  private [this] var happensAfter: Option[HLCTimestamp] = None
  
  def buildTranaction(transactionUUID: UUID): (TransactionDescription, Map[DataStoreID, List[LocalUpdate]]) = synchronized {
    val startTimestamp = happensAfter match {
      case Some(ts) => HLCTimestamp.happensAfter(ts)
      case None => HLCTimestamp.now
    }
    val primaryObject = requirements.map(_.objectPointer).maxBy(ptr => ptr.ida)
    val designatedLeaderUID = chooseDesignatedLeader(primaryObject)
    val originatingClient = Some(clientId)
    
    val txd = TransactionDescription(transactionUUID, startTimestamp.asLong, primaryObject, designatedLeaderUID, 
                                     requirements, finalizationActions, originatingClient, notifyOnResolution.toList)
           
    var updates = Map[DataStoreID, List[LocalUpdate]]()
    
    objectUpdates foreach { t => 
      val (objectPointer, buf) = t
      
      val encoded = objectPointer.ida.encode(buf) 
      val idx2buff = objectPointer.storePointers zip encoded foreach { x =>
        val (sp, bb) = x
        val storeId = DataStoreID(objectPointer.poolUUID, sp.poolIndex)
        val lu = LocalUpdate(objectPointer.uuid, bb)
        
        updates.get(storeId) match {
          case None => updates += (storeId -> List(lu))
          case Some(lst) => 
            val newList = lu :: lst
            updates += (storeId -> newList)
        }
      }
    } 
                                     
    (txd, updates)
  }
  
  def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = synchronized {
    if (objectUpdates.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    
    objectUpdates += (objectPointer -> data)
    requirements = DataUpdate(objectPointer, requiredRevision, DataUpdateOperation.Append) :: requirements
    
    requiredRevision.append(data.size)
  }
  
  def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): ObjectRevision = synchronized {
    if (objectUpdates.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    
    objectUpdates += (objectPointer -> data)
    requirements  = DataUpdate(objectPointer, requiredRevision, DataUpdateOperation.Overwrite) :: requirements
    
    requiredRevision.overwrite(data.size)
  }
  
  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount = synchronized {
    if (refcountUpdates.contains(objectPointer))
      throw MultipleRefcountUpdatesToObject(objectPointer)
    
    refcountUpdates += objectPointer
    requirements  = RefcountUpdate(objectPointer, requiredRefcount, refcount) :: requirements
    
    refcount
  }
  
  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): ObjectRevision = synchronized {
    if (objectUpdates.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    
    requirements = VersionBump(objectPointer, requiredRevision) :: requirements
    
    requiredRevision.versionBump()
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