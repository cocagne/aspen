package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.objects.StorePointer
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision
import com.ibm.aspen.core.data_store.DataStoreID
import com.ibm.aspen.core.HLCTimestamp
import com.ibm.aspen.core.objects.ObjectType

case class AllocationRecoveryState(
  storeId: DataStoreID,
  storePointer: StorePointer,
  newObjectUUID: UUID,
  objectType: ObjectType.Value,
  objectSize: Option[Int],
  objectData: DataBuffer,
  initialRefcount: ObjectRefcount,
  timestamp: HLCTimestamp,
  allocationTransactionUUID: UUID,
  allocatingObject: ObjectPointer,
  allocatingObjectRevision: ObjectRevision 
)