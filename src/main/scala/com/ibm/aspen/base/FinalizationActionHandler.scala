package com.ibm.aspen.base

import java.util.UUID
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.transaction.TransactionDescription


trait FinalizationActionHandler extends TypeFactory {
  
  val typeUUID: UUID
  
  def createAction(
      system: AspenSystem,
      txd: TransactionDescription,
      serializedActionData: Array[Byte], 
      successfullyUpdatedPeers: Set[DataStoreID]): FinalizationAction
}