package com.ibm.aspen.base

import java.util.UUID
import com.ibm.aspen.base.kvtree.KVTreeFinalizationActions

object FinalizationActionRegistry extends FinalizationActionHandler {
  val supportedUUIDs: Set[UUID] = KVTreeFinalizationActions.supportedUUIDs
  
  val actionMap: Map[UUID, FinalizationActionHandler] = KVTreeFinalizationActions.supportedUUIDs.map( (_ -> KVTreeFinalizationActions) ).toMap
  
  
  def createAction(finalizationActionUUID: UUID, serializedActionData: Array[Byte]): Option[FinalizationAction] = {
    actionMap.get(finalizationActionUUID) match {
      case None => None
      case Some(handler) => handler.createAction(finalizationActionUUID, serializedActionData)
    }
  }
}