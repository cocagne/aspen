package com.ibm.aspen.base

import java.util.UUID


trait FinalizationActionHandler {
  val supportedUUIDs: Set[UUID]
  
  def createAction(
      finalizationActionUUID: UUID, 
      serializedActionData: Array[Byte]): Option[FinalizationAction]
}