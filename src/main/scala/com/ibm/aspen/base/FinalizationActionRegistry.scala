package com.ibm.aspen.base

import java.util.UUID
import com.ibm.aspen.base.kvtree.KVTreeFinalizationActionHandler

class FinalizationActionRegistry(handlers: List[FinalizationActionHandler]) extends FinalizationActionHandler {
  
  val supportedUUIDs: Set[UUID] = handlers.foldLeft(Set[UUID]())( (s, h) => s ++ h.supportedUUIDs )  
  
  val actionMap: Map[UUID, FinalizationActionHandler] = handlers.foldLeft(Map[UUID, FinalizationActionHandler]()) {
    (m, h) => h.supportedUUIDs.foldLeft(m)( (m2, uuid) => m2 + (uuid -> h))
  }
  
  def createAction(
      finalizationActionUUID: UUID, 
      serializedActionData: Array[Byte]): Option[FinalizationAction] = {
    actionMap.get(finalizationActionUUID) match {
      case None => None
      case Some(handler) => handler.createAction(finalizationActionUUID, serializedActionData)
    }
  }
}