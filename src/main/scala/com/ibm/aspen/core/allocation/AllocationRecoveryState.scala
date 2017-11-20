package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.objects.StorePointer
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.data_store.DataStoreID

object AllocationRecoveryState { 
  case class NewObject(
      storePointer: StorePointer,
      newObjectUUID: UUID,
      objectSize: Option[Int],
      objectData: DataBuffer,
      initialRefcount: ObjectRefcount)
}
case class AllocationRecoveryState(
  storeId: DataStoreID,
  newObjects: List[AllocationRecoveryState.NewObject],
  allocationTransactionUUID: UUID,
  allocatingObject: ObjectPointer,
  allocatingObjectRevision: ObjectRevision 
)