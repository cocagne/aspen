package com.ibm.aspen.core.allocation

import com.ibm.aspen.core.objects.StorePointer
import java.util.UUID
import com.ibm.aspen.core.DataBuffer
import com.ibm.aspen.core.objects.ObjectRefcount
import com.ibm.aspen.core.objects.ObjectPointer
import com.ibm.aspen.core.objects.ObjectRevision

case class AllocationRecoveryState(
  storePointer: StorePointer,
  newObjectUUID: UUID,
  objectSize: Option[Int],
  objectData: DataBuffer,
  initialRefcount: ObjectRefcount,
  allocationTransactionUUID: UUID,
  allocatingObject: ObjectPointer,
  allocatingObjectRevision: ObjectRevision 
)