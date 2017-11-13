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

class TransactionBuilder(
    chooseDesignatedLeader: (ObjectPointer) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
    clientId: ClientID) {
  
  private [this] var dataObjects = Set[ObjectPointer]()
  private [this] var refcountObjects = Set[ObjectPointer]()
  private [this] var dataUpdates = List[DataUpdate]()
  private [this] var dataBuffers = List[ByteBuffer]()
  private [this] var refcountUpdates = List[RefcountUpdate]()
  private [this] var finalizationActions = List[SerializedFinalizationAction]()
  
  def buildTranaction(transactionUUID: UUID): (TransactionDescription, Map[DataStoreID, List[LocalUpdate]]) = synchronized {
    val startTimestamp = System.currentTimeMillis()
    val primaryObject = (dataObjects.iterator ++ refcountObjects.iterator).maxBy(ptr => ptr.ida)
    val designatedLeaderUID = chooseDesignatedLeader(primaryObject)
    val originatingClient = Some(clientId)
    
    val txd = TransactionDescription(transactionUUID, startTimestamp, primaryObject, designatedLeaderUID, 
                                     dataUpdates, refcountUpdates, finalizationActions, originatingClient)
           
    var updates = Map[DataStoreID, List[LocalUpdate]]()
    
    val encodedUpdates = dataUpdates zip dataBuffers map { t => 
      val (du, buf) = t
      
      val encoded = du.objectPointer.ida.encode(buf.asReadOnlyBuffer()) 
      val idx2buff = du.objectPointer.storePointers zip encoded foreach { x =>
        val (sp, bb) = x
        val storeId = DataStoreID(du.objectPointer.poolUUID, sp.poolIndex)
        val lu = LocalUpdate(du.objectPointer.uuid, bb)
        
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
  
  def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: ByteBuffer): ObjectRevision = synchronized {
    if (dataObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    
    dataObjects += objectPointer
    dataUpdates  = DataUpdate(objectPointer, requiredRevision, DataUpdateOperation.Append) :: dataUpdates
    dataBuffers  = data.asReadOnlyBuffer() :: dataBuffers
    
    requiredRevision.append(data.limit() - data.position())
  }
  
  def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: ByteBuffer): ObjectRevision = synchronized {
    if (dataObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    
    dataObjects += objectPointer
    dataUpdates  = DataUpdate(objectPointer, requiredRevision, DataUpdateOperation.Overwrite) :: dataUpdates
    dataBuffers  = data.asReadOnlyBuffer() :: dataBuffers
    
    requiredRevision.overwrite(data.limit() - data.position())
  }
  
  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): ObjectRefcount = synchronized {
    if (refcountObjects.contains(objectPointer))
      throw MultipleRefcountUpdatesToObject(objectPointer)
    
    refcountObjects += objectPointer
    refcountUpdates  = RefcountUpdate(objectPointer, requiredRefcount, refcount) :: refcountUpdates
    
    refcount
  }
  
  def addFinalizationAction(finalizationActionUUID: UUID, serializedContent: Array[Byte]): Unit = synchronized {
    finalizationActions = SerializedFinalizationAction(finalizationActionUUID, serializedContent) :: finalizationActions
  }
}