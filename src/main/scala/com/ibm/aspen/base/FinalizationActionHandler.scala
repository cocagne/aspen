package com.ibm.aspen.base

import java.util.UUID


trait FinalizationActionHandler extends TypeFactory {
  
  val typeUUID: UUID
  
  def createAction(serializedActionData: Array[Byte]): FinalizationAction
}