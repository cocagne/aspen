package com.ibm.aspen.base.impl

import java.util.UUID
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.base.Transaction
import com.ibm.aspen.base.FinalizationActionHandler
import com.ibm.aspen.base.AspenSystem
import com.ibm.aspen.base.FinalizationAction
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TransactionDescription
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

object MissedUpdateFinalizationAction {
  val MissedUpdateFinalizationActionUUID = UUID.fromString("368e7176-8fed-4c61-bb8c-d1554722c5d1")
  
  case class MissedUpdate(pointer: ObjectPointer, stores: List[Byte])
  
  def addAction(transaction: Transaction): Unit = transaction.addFinalizationAction(MissedUpdateFinalizationActionUUID)
  
  class MissedUpdateHandler(
      val system: AspenSystem, 
      val txd: TransactionDescription, 
      val successfullyUpdatedPeers: Set[DataStoreID]) extends FinalizationAction {
    
    def execute()(implicit ec: ExecutionContext): Future[Unit] = {
      val allObjects = txd.allReferencedObjectsSet
      
      val misses = allObjects.foldLeft(List[MissedUpdate]()) { (l, p) =>
        val storeSet = p.storePointers.map(sp => DataStoreID(p.poolUUID, sp.poolIndex)).toSet
        val misses = storeSet -- successfullyUpdatedPeers
        
        if (misses.isEmpty)
          l
        else
          MissedUpdate(p, misses.map(storeId => storeId.poolIndex).toList) :: l
      }
      
      if (misses.isEmpty)
        Future.unit
      else
        logMisses(misses)
    }
  }
  
  // 
  def logMisses(misses: List[MissedUpdate])(implicit ec: ExecutionContext): Future[Unit] = Future.unit
}

class MissedUpdateFinalizationAction extends FinalizationActionHandler {
  
  import MissedUpdateFinalizationAction._
  
  val typeUUID: UUID = MissedUpdateFinalizationActionUUID
  
  override def createAction(
      system: AspenSystem, 
      txd: TransactionDescription,
      serializedActionData: Array[Byte], 
      successfullyUpdatedPeers: Set[DataStoreID]): FinalizationAction = {

    val fa = BaseCodec.decodeFinalizationActionContent(serializedActionData)
    
    new MissedUpdateHandler(system, txd, successfullyUpdatedPeers)      
  }
  
}
