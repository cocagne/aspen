package com.ibm.aspen.base

import java.util.UUID


trait FinalizationActionHandler {
  
  val finalizationActionUUID: UUID

  def createAction(serializedActionData: Array[Byte]): FinalizationAction
}